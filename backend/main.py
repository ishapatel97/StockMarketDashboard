from fastapi import FastAPI, Query
from typing import List
from dotenv import load_dotenv
load_dotenv()
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import logging
from datetime import datetime

from app.services.stock_service import (
    ingest_next_batch,
    ingest_all_tickers_fast,
    ingest_latest_prices,
    get_top_stocks_from_db,
    get_ingest_progress,
    get_chart_data,
    get_all_sectors,
    refresh_calculated_stocks,
)
from app.services.ai_service import get_ai_reason
from database import Base, engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ET = pytz.timezone("America/New_York")


def is_market_open() -> bool:
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return False
    market_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return market_open <= now <= market_close


def scheduled_latest_ingest():
    """Every 4 hours: fetch last 2 days prices + metadata, then recalculate."""
    logger.info("Running scheduled latest ingest (4h)...")
    result = ingest_latest_prices()
    logger.info(f"Scheduled latest ingest done: {result}")


def market_open_ingest():
    """At market open: fetch latest prices + metadata, then recalculate."""
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return
    logger.info("Market open ingest starting...")
    result = ingest_latest_prices()
    logger.info(f"Market open ingest done: {result}")


scheduler = BackgroundScheduler(timezone=ET)

# Fetch latest prices + metadata every 4 hours
# (ingest_latest_prices auto-triggers refresh_calculated_stocks when done)
scheduler.add_job(
    scheduled_latest_ingest,
    CronTrigger(hour="*/4"),
    id="ingest_latest_4h",
    replace_existing=True,
)

# Also fetch at market open Mon–Fri 9:30 AM ET
scheduler.add_job(
    market_open_ingest,
    CronTrigger(day_of_week="mon-fri", hour=9, minute=30, timezone=ET),
    id="market_open_ingest",
    replace_existing=True,
)


# Known clean yfinance sector names
YFINANCE_SECTORS = {
    "Technology", "Healthcare", "Financial Services", "Consumer Cyclical",
    "Industrials", "Communication Services", "Consumer Defensive", "Energy",
    "Basic Materials", "Real Estate", "Utilities",
}


def cleanup_polygon_sectors():
    """
    One-time fix: Polygon wrote SIC descriptions (e.g. 'Pharmaceutical Preparations')
    over the clean yfinance sectors (e.g. 'Healthcare'). This restores them.
    For each symbol, find the yfinance sector from older rows and apply it everywhere.
    """
    from sqlalchemy import text as sa_text
    from database import engine as db_engine

    logger.info("Checking for Polygon SIC sectors to clean up...")
    with db_engine.connect() as conn:
        # Get all distinct sectors currently in DB
        rows = conn.execute(sa_text(
            "SELECT DISTINCT sector FROM stock_prices WHERE sector IS NOT NULL AND sector != ''"
        )).fetchall()
        current_sectors = {r[0] for r in rows}

        # Find sectors that are NOT in the yfinance list (these are Polygon SIC descriptions)
        bad_sectors = current_sectors - YFINANCE_SECTORS
        if not bad_sectors:
            logger.info("No Polygon SIC sectors found — all clean.")
            return

        logger.info(f"Found {len(bad_sectors)} non-yfinance sectors to fix: {list(bad_sectors)[:5]}...")

        # For each symbol that has a bad sector, try to find a good yfinance sector from other rows
        for bad_sector in bad_sectors:
            # Get symbols with this bad sector
            syms = conn.execute(sa_text(
                "SELECT DISTINCT symbol FROM stock_prices WHERE sector = :bad"
            ), {"bad": bad_sector}).fetchall()

            for (sym,) in syms:
                # Check if this symbol has a good yfinance sector in any other row
                good = conn.execute(sa_text("""
                    SELECT sector FROM stock_prices
                    WHERE symbol = :sym AND sector IS NOT NULL AND sector != '' AND sector != :bad
                    LIMIT 1
                """), {"sym": sym, "bad": bad_sector}).fetchone()

                if good and good[0] in YFINANCE_SECTORS:
                    # Restore the good sector
                    conn.execute(sa_text(
                        "UPDATE stock_prices SET sector = :good WHERE symbol = :sym AND sector = :bad"
                    ), {"good": good[0], "sym": sym, "bad": bad_sector})

        # Also clean up calculated_stocks
        conn.execute(sa_text("""
            UPDATE calculated_stocks cs SET sector = sp.sector
            FROM (
                SELECT DISTINCT ON (symbol) symbol, sector
                FROM stock_prices
                WHERE sector IS NOT NULL AND sector != ''
                ORDER BY symbol, date DESC
            ) sp
            WHERE cs.symbol = sp.symbol AND cs.sector != sp.sector
        """))

        conn.commit()
        logger.info("Polygon SIC sectors cleaned up successfully.")


def ensure_indexes():
    """Create indexes on stock_prices and calculated_stocks for fast queries."""
    from sqlalchemy import text as sa_text
    from database import engine as db_engine
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_sp_symbol_date ON stock_prices(symbol, date DESC)",
        "CREATE INDEX IF NOT EXISTS idx_sp_sector ON stock_prices(sector) WHERE sector IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_sp_market_cap ON stock_prices(market_cap) WHERE market_cap IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_cs_symbol ON calculated_stocks(symbol)",
        "CREATE INDEX IF NOT EXISTS idx_cs_volume_surge ON calculated_stocks(volume_surge DESC)",
    ]
    with db_engine.connect() as conn:
        for idx_sql in indexes:
            try:
                conn.execute(sa_text(idx_sql))
            except Exception as e:
                logger.warning(f"Index creation skipped: {e}")
        conn.commit()
    logger.info("Database indexes ensured.")


def startup_refresh():
    """If calculated_stocks is empty, populate it immediately so the UI has data."""
    from database import engine as db_engine
    from sqlalchemy import text as sa_text
    with db_engine.connect() as conn:
        count = conn.execute(sa_text("SELECT COUNT(*) FROM calculated_stocks")).scalar()
    if count == 0:
        logger.info("calculated_stocks is EMPTY — running initial refresh...")
        refresh_calculated_stocks()
    else:
        logger.info(f"calculated_stocks has {count} rows — skipping startup refresh.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure indexes exist for fast queries
    try:
        ensure_indexes()
    except Exception as e:
        logger.warning(f"Index creation failed (non-fatal): {e}")

    # Clean up Polygon SIC sectors that overwrote yfinance sectors
    try:
        cleanup_polygon_sectors()
    except Exception as e:
        logger.warning(f"Sector cleanup failed (non-fatal): {e}")

    scheduler.start()
    logger.info("Scheduler started.")

    # Auto-populate calculated_stocks if empty (runs in background thread)
    import threading
    threading.Thread(target=startup_refresh, daemon=True).start()

    yield
    scheduler.shutdown()
    logger.info("Scheduler stopped.")


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Base.metadata.create_all(bind=engine)


@app.get("/")
def home():
    return {"message": "Welcome to the Stock Market Dashboard!"}


@app.get("/sectors")
def sectors():
    """Return all distinct sectors from DB."""
    return get_all_sectors()


@app.get("/stocks")
def stocks_from_db(
    threshold: float     = Query(1.5),
    limit:     int       = Query(50, ge=5, le=1000),
    sectors:   List[str] = Query(default=[]),
):
    """
    Fast endpoint — reads from calculated_stocks (pre-calculated every 10h).
    Returns volume_surge, price_change, avg_volume, stock_insight instantly.
    JOINs stock_prices latest row for current price, company, sector, market_cap.
    """
    sector_filter = sectors if sectors else None
    return get_top_stocks_from_db(
        min_volume_surge_pct=threshold,
        limit=limit,
        sectors=sector_filter,
    )


@app.get("/top-stocks")
def top_stocks():
    return get_top_stocks_from_db()


@app.get("/chart/{symbol}")
def get_chart(symbol: str):
    return get_chart_data(symbol)


@app.post("/ingest-batch")
def ingest_batch():
    return ingest_next_batch()


@app.post("/ingest-all")
def ingest_all():
    return ingest_all_tickers_fast()


@app.post("/ingest-latest")
def ingest_latest():
    """Fetch last 2 days prices + company/market_cap/sector for all symbols, then recalculate."""
    import threading
    threading.Thread(target=ingest_latest_prices, daemon=True).start()
    return {"status": "ok", "message": "ingest_latest_prices started in background"}


@app.get("/ingest-progress")
def ingest_progress():
    return get_ingest_progress()


@app.post("/refresh-summary")
def refresh_summary_manual():
    """Manually trigger a recalculation of calculated_stocks."""
    refresh_calculated_stocks()
    return {"status": "ok", "message": "calculated_stocks refreshed successfully"}


@app.get("/insight/{symbol}")
def insight(
    symbol: str,
    price: float = Query(0),
    price_change: float = Query(0),
    volume_surge: float = Query(0),
    market_cap_billion: float = Query(0),
):
    """Brief 1-2 sentence AI insight for a stock card."""
    from app.services.ai_service import get_brief_insight, _GROQ_DAILY_EXHAUSTED
    # If daily limit already hit, return immediately without calling Groq
    if _GROQ_DAILY_EXHAUSTED.get("exhausted"):
        return {"symbol": symbol, "insight": "", "rate_limited": True}
    result = get_brief_insight(symbol, price, price_change, volume_surge, market_cap_billion)
    limited = _GROQ_DAILY_EXHAUSTED.get("exhausted", False)
    return {"symbol": symbol, "insight": result, "rate_limited": limited}


@app.get("/reason/{symbol}")
def reason(symbol: str, threshold: float = Query(1.5)):
    """Full AI summary with news sources — shown in chart modal."""
    return get_ai_reason(symbol, threshold=threshold)


@app.get("/scheduler-status")
def scheduler_status():
    jobs = [{"id": job.id, "next_run": str(job.next_run_time)} for job in scheduler.get_jobs()]
    return {"market_open": is_market_open(), "scheduler_running": scheduler.running, "jobs": jobs}
from fastapi import FastAPI, Query
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
    get_top_stocks_from_db,
    get_ingest_progress,
    backfill_company_missing,
    backfill_market_cap,
    get_chart_data,
)
from app.services.ai_service import get_ai_reason
from database import Base, engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ET = pytz.timezone("America/New_York")


def is_market_open() -> bool:
    """Returns True if US market is currently open (Mon-Fri 9:30-16:00 ET)."""
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return False
    market_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return market_open <= now <= market_close


def scheduled_ingest():
    """Ingest latest data — runs every 15 min during market hours."""
    #if not is_market_open():
      #  logger.info("Market closed — skipping scheduled ingest.")
       # return
    logger.info("Market open — running scheduled ingest...")
    result = ingest_all_tickers_fast()
    logger.info(f"Scheduled ingest done: {result}")


def market_open_ingest():
    """Full refresh at market open (9:30 AM ET, Mon-Fri)."""
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return
    logger.info("Market open refresh starting...")
    result = ingest_all_tickers_fast()
    logger.info(f"Market open ingest done: {result}")


scheduler = BackgroundScheduler(timezone=ET)

scheduler.add_job(
    scheduled_ingest,
    CronTrigger(minute="*/5"),
    id="ingest_15min",
    replace_existing=True,
)

scheduler.add_job(
    market_open_ingest,
    CronTrigger(day_of_week="mon-fri", hour=9, minute=30, timezone=ET),
    id="market_open_ingest",
    replace_existing=True,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    logger.info("Scheduler started — ingest runs every 15min during market hours.")
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


@app.get("/ingest-progress")
def ingest_progress():
    return get_ingest_progress()


@app.get("/stocks")
def stocks_from_db(
    threshold: float = Query(1.5, description="Minimum % by which today's volume must exceed 20d avg"),
    limit: int = Query(20, ge=5, le=50),
):
    return get_top_stocks_from_db(min_volume_surge_pct=threshold, limit=limit)


@app.post("/backfill-company")
def backfill_company():
    return backfill_company_missing()

@app.post("/backfill-marketcap")
def backfill_marketcap():
    return backfill_market_cap()
@app.get("/scheduler-status")
def scheduler_status():
    """Check scheduler status and next run times."""
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "next_run": str(job.next_run_time),
        })
    return {
        "market_open": is_market_open(),
        "scheduler_running": scheduler.running,
        "jobs": jobs,
    }


@app.get("/reason/{symbol}")
def reason(symbol: str, threshold: float = Query(1.5)):
    return get_ai_reason(symbol, threshold=threshold)

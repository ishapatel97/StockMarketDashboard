from fastapi import FastAPI, Query
from typing import List, Optional
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
    get_chart_data,
    get_all_sectors,
)
from app.services.ai_service import get_ai_reason, get_brief_insight
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


def scheduled_ingest():
    logger.info("Running scheduled ingest...")
    result = ingest_all_tickers_fast()
    logger.info(f"Scheduled ingest done: {result}")


def market_open_ingest():
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return
    logger.info("Market open refresh starting...")
    result = ingest_all_tickers_fast()
    logger.info(f"Market open ingest done: {result}")


scheduler = BackgroundScheduler(timezone=ET)
scheduler.add_job(scheduled_ingest,    CronTrigger(minute="*/5"),                                          id="ingest_15min",        replace_existing=True)
scheduler.add_job(market_open_ingest,  CronTrigger(day_of_week="mon-fri", hour=9, minute=30, timezone=ET), id="market_open_ingest",   replace_existing=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    logger.info("Scheduler started.")
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
    """Return all distinct sectors available in DB."""
    return get_all_sectors()


@app.get("/stocks")
def stocks_from_db(
    threshold:  float         = Query(1.5),
    limit:      int           = Query(50, ge=5, le=1000),
    sectors:    List[str]     = Query(default=[]),
):
    """
    Returns top stocks filtered by volume surge threshold and optional sectors.
    sectors is a repeated query param: /stocks?sectors=Technology&sectors=Healthcare
    limit defaults to 50 so sector filter applies across all meaningful data.
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


@app.get("/ingest-progress")
def ingest_progress():
    return get_ingest_progress()


@app.get("/reason/{symbol}")
def reason(symbol: str, threshold: float = Query(1.5)):
    return get_ai_reason(symbol, threshold=threshold)


@app.get("/brief-insight/{symbol}")
def brief_insight(
    symbol:             str,
    price:              float = Query(...),
    price_change:       float = Query(...),
    volume_surge:       float = Query(...),
    market_cap_billion: float = Query(0.0),
):
    text = get_brief_insight(symbol, price, price_change, volume_surge, market_cap_billion)
    return {"symbol": symbol, "insight": text}


@app.get("/scheduler-status")
def scheduler_status():
    jobs = [{"id": job.id, "next_run": str(job.next_run_time)} for job in scheduler.get_jobs()]
    return {"market_open": is_market_open(), "scheduler_running": scheduler.running, "jobs": jobs}
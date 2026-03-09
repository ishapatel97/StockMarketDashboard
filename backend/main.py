from fastapi import FastAPI, Query
from dotenv import load_dotenv
load_dotenv()
from fastapi.middleware.cors import CORSMiddleware
from database import Base, engine
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from database import SessionLocal
app = FastAPI()

from app.services.stock_service import (
    ingest_next_batch,
    ingest_all_tickers_fast,
    get_top_stocks_from_db,
    get_ingest_progress,
    backfill_company_missing,
    get_chart_data,
)
from app.services.ai_service import get_ai_reason

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


@app.get("/reason/{symbol}")
def reason(symbol: str, threshold: float = Query(1.5)):
    return get_ai_reason(symbol, threshold=threshold)
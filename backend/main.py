from fastapi import FastAPI, Query
from dotenv import load_dotenv
load_dotenv()
from fastapi.middleware.cors import CORSMiddleware
from app.services.stock_service import (
    ingest_next_batch,
    ingest_all_tickers_fast,
    get_top_stocks_from_db,
    get_ingest_progress,
    backfill_company_missing,
)
from app.services.ai_service import get_ai_reason
import yfinance as yf
from database import Base, engine

app = FastAPI()

# Add CORS middleware
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

# Deprecated: old endpoint; kept for backward compatibility if needed
@app.get("/top-stocks")
def top_stocks():
    return get_top_stocks_from_db()


@app.get("/chart/{symbol}")
def get_chart(symbol: str):
    stock = yf.Ticker(symbol)
    hist = stock.history(period="20d")

    # Safely handle NaNs for price/volume
    prices = hist["Close"].ffill().bfill().tolist() if "Close" in hist.columns else []
    volumes = hist["Volume"].fillna(0).tolist() if "Volume" in hist.columns else []

    return {
        "symbol": symbol,
        "dates": [date.strftime("%Y-%m-%d") for date in hist.index],
        "prices": prices,
        "volumes": volumes,
    }


# New endpoints
@app.post("/ingest-batch")
def ingest_batch():
    return ingest_next_batch()


@app.post("/ingest-all")
def ingest_all():
    # Kick off fast ingestion synchronously; returns when finished
    return ingest_all_tickers_fast()


@app.get("/ingest-progress")
def ingest_progress():
    return get_ingest_progress()


@app.get("/stocks")
def stocks_from_db(threshold: float = Query(1.5, description="Minimum % by which today's volume must exceed 20d avg"),
                   limit: int = Query(20, ge=5, le=50)):
    return get_top_stocks_from_db(min_volume_surge_pct=threshold, limit=limit)


# Temporary endpoint to backfill missing company names
@app.post("/backfill-company")
def backfill_company():
    return backfill_company_missing()


@app.get("/reason/{symbol}")
def reason(symbol: str, threshold: float = Query(1.5)):
    return get_ai_reason(symbol, threshold=threshold)


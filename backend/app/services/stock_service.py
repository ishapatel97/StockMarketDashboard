import yfinance as yf
from app.services.universe_service import load_tickers
from concurrent.futures import ThreadPoolExecutor
import time
import json
import os
from datetime import datetime, timedelta
from database import SessionLocal
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
import pandas as pd
from threading import Lock

# Cache file path
CACHE_FILE = "stock_cache.json"
BATCH_INDEX_FILE = "batch_index.json"
CACHE_DURATION = 3600  # 1 hour in seconds
BATCH_SIZE = 200

# In-memory progress tracker for ingestion
_INGEST_PROGRESS = {
    "started": False,
    "done": False,
    "total_tickers": 0,
    "processed_tickers": 0,
    "rows_inserted": 0,
    "current_chunk": 0,
    "total_chunks": 0,
}
_PROGRESS_LOCK = Lock()

def load_cache():
    """Load cached stock data if it exists and is fresh"""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                cache_data = json.load(f)
                cache_time = datetime.fromisoformat(cache_data.get('timestamp', ''))
                if datetime.now() - cache_time < timedelta(seconds=CACHE_DURATION):
                    print(f"Using cached data from {cache_time}")
                    return cache_data.get('stocks', [])
        except:
            pass
    return None

def save_cache(stocks):
    """Save stock data to cache"""
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'stocks': stocks
            }, f)
    except:
        pass

def load_batch_index():
    """Load the current batch index"""
    if os.path.exists(BATCH_INDEX_FILE):
        try:
            with open(BATCH_INDEX_FILE, 'r') as f:
                data = json.load(f)
                return data.get('batch_index', 0)
        except:
            pass
    return 0

def save_batch_index(batch_index):
    """Save the current batch index"""
    try:
        with open(BATCH_INDEX_FILE, 'w') as f:
            json.dump({'batch_index': batch_index}, f)
    except:
        pass

def get_next_batch(all_tickers):
    """Get the next batch of tickers and update the batch index"""
    total_tickers = len(all_tickers)
    current_batch = load_batch_index()
    
    start_index = current_batch * BATCH_SIZE
    end_index = start_index + BATCH_SIZE
    
    # If we've reached the end, wrap around to the beginning
    if start_index >= total_tickers:
        current_batch = 0
        start_index = 0
        end_index = BATCH_SIZE
    
    # Get the batch
    tickers = all_tickers[start_index:end_index]
    
    # Update batch index for next time
    next_batch = current_batch + 1
    save_batch_index(next_batch)
    
    return tickers, start_index, current_batch

def _get_company_for_symbol(symbol: str) -> str:
    try:
        info = yf.Ticker(symbol).info
        return info.get("longName") or info.get("shortName") or info.get("name") or ""
    except Exception:
        return ""


def _backfill_company_symbol(db, symbol: str, company: str):
    if not company:
        return
    db.execute(text(
        "UPDATE stock_prices SET company=:company WHERE symbol=:symbol AND (company IS NULL OR company='')"
    ), {"company": company, "symbol": symbol})


def save_history(symbol, hist):
    """Persist raw history rows with no calculation. Create rows idempotently. Also store company."""
    db = SessionLocal()
    try:
        company = _get_company_for_symbol(symbol)
        for date, row in hist.iterrows():
            db.execute(text(
                """
                INSERT INTO stock_prices(symbol, date, close_price, volume, company)
                VALUES(:symbol, :date, :price, :volume, :company)
                ON CONFLICT(symbol, date) DO NOTHING
                """
            ), {
                "symbol": symbol,
                "date": date.to_pydatetime() if hasattr(date, 'to_pydatetime') else date,
                "price": float(row.get("Close")) if row.get("Close") == row.get("Close") else None,
                "volume": int(row.get("Volume")) if row.get("Volume") == row.get("Volume") else None,
                "company": company,
            })
        # Backfill older rows for this symbol missing company
        _backfill_company_symbol(db, symbol, company)
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        raise
    finally:
        db.close()

def analyze_stock(ticker):
    """Ensure DB has latest raw data for ticker; then compute metrics from DB only."""
    time.sleep(0.2)
    try:
        db = SessionLocal()
        # Pull last 40 rows from DB first
        result = db.execute(text(
            """
            SELECT date, close_price, volume
            FROM stock_prices
            WHERE symbol = :symbol
            ORDER BY date DESC
            LIMIT 40
            """
        ), {"symbol": ticker}).fetchall()
        db.close()

        need_rows = len(result) < 20
        if need_rows:
            # Ingest fresh raw data only; no calculations here
            stock = yf.Ticker(ticker)
            hist = stock.history(period="2mo")
            if hist is not None and not hist.empty:
                save_history(ticker, hist)
            # Re-read from DB after ingestion
            db = SessionLocal()
            result = db.execute(text(
                """
                SELECT date, close_price, volume
                FROM stock_prices
                WHERE symbol = :symbol
                ORDER BY date ASC
                LIMIT 40
                """
            ), {"symbol": ticker}).fetchall()
            db.close()
        else:
            # Use DB data and order ASC for time series ops
            result = list(result)[::-1]

        if not result or len(result) < 2:
            return None

        hist = pd.DataFrame(result, columns=["Date", "Close", "Volume"]).sort_values("Date")

        volumes = hist["Volume"].dropna()
        if len(volumes) < 20:
            return None
        avg_volume = volumes.rolling(20).mean().iloc[-1]
        today_volume = volumes.iloc[-1]
        if not pd.notna(avg_volume) or avg_volume == 0:
            return None
        if avg_volume < 100000:
            return None
        if today_volume < 20000:
            return None
        volume_surge = ((today_volume - avg_volume) / avg_volume) * 100

        prices = hist["Close"].dropna()
        if len(prices) < 2:
            return None
        prev_close = prices.iloc[-2]
        price = prices.iloc[-1]
        if price < 5:
            return None
        price_change = ((price - prev_close) / prev_close) * 100

        return {
            "symbol": ticker,
            "price": float(round(price, 2)),
            "price_change": float(round(price_change, 2)),
            "market_cap_billion": 0,
            "today_volume": int(today_volume),
            "avg_volume": int(avg_volume),
            "volume_surge": float(round(volume_surge, 2)),
        }
    except Exception:
        return None

def ingest_next_batch():
    """Ingest raw API data for the next batch only; do not compute metrics."""
    all_tickers = load_tickers()
    tickers, start_index, batch_num = get_next_batch(all_tickers)

    processed = []
    for idx, ticker in enumerate(tickers, start=1):
        # Check if we need more rows for this ticker
        db = SessionLocal()
        count = db.execute(text(
            "SELECT COUNT(*) FROM stock_prices WHERE symbol = :symbol"
        ), {"symbol": ticker}).scalar() or 0
        db.close()

        if count < 20:
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(period="2mo")
                if hist is not None and not hist.empty:
                    save_history(ticker, hist)
                    processed.append({"symbol": ticker, "ingested": True, "rows_before": int(count), "progress": f"{idx}/{len(tickers)}"})
                else:
                    processed.append({"symbol": ticker, "ingested": False, "rows_before": int(count), "progress": f"{idx}/{len(tickers)}"})
            except Exception:
                processed.append({"symbol": ticker, "ingested": False, "rows_before": int(count), "progress": f"{idx}/{len(tickers)}"})
        else:
            processed.append({"symbol": ticker, "ingested": False, "rows_before": int(count), "progress": f"{idx}/{len(tickers)}"})

    return {
        "batch": batch_num,
        "start_index": start_index,
        "tickers": tickers,
        "processed": processed,
        "total": len(tickers),
    }


def _set_progress(**kwargs):
    with _PROGRESS_LOCK:
        _INGEST_PROGRESS.update(kwargs)

def get_ingest_progress():
    with _PROGRESS_LOCK:
        return dict(_INGEST_PROGRESS)


def _chunk_list(items, size):
    for i in range(0, len(items), size):
        yield items[i:i+size]


def _bulk_insert_rows(rows):
    if not rows:
        return 0
    db = SessionLocal()
    try:
        db.execute(text(
            """
            INSERT INTO stock_prices(symbol, date, close_price, volume, company)
            VALUES(:symbol, :date, :price, :volume, :company)
            ON CONFLICT(symbol, date) DO NOTHING
            """
        ), rows)
        # Backfill per symbol for missing company in older rows
        symbols = {r["symbol"] for r in rows if r.get("company")}
        for sym in symbols:
            comp = next((r["company"] for r in rows if r["symbol"] == sym and r.get("company")), "")
            if comp:
                _backfill_company_symbol(db, sym, comp)
        db.commit()
        return len(rows)
    except Exception:
        db.rollback()
        return 0
    finally:
        db.close()


def ingest_all_tickers_fast(chunk_size: int = 100, max_workers: int = 10):
    """Fast full ingestion using grouped downloads, parallel chunks, and bulk inserts."""
    all_tickers = load_tickers()

    # Preload existing counts once
    db = SessionLocal()
    counts = db.execute(text("SELECT symbol, COUNT(*) FROM stock_prices GROUP BY symbol")).fetchall()
    db.close()
    have_enough = {sym for sym, c in counts if (c or 0) >= 20}

    remaining = [t for t in all_tickers if t not in have_enough]

    total = len(remaining)
    chunks = list(_chunk_list(remaining, chunk_size))
    total_chunks = len(chunks)

    _set_progress(started=True, done=False, total_tickers=total, processed_tickers=0, rows_inserted=0, current_chunk=0, total_chunks=total_chunks)

    def process_chunk(idx, tick_chunk):
        # Grouped download for the chunk
        try:
            data = yf.download(tickers=tick_chunk, period="2mo", group_by='ticker', threads=True, auto_adjust=False, progress=False)
        except Exception:
            data = None
        rows = []
        if data is None or data.empty:
            with _PROGRESS_LOCK:
                _INGEST_PROGRESS["current_chunk"] = idx + 1
            return 0, len(tick_chunk)

        # data can be multi-indexed DF when group_by='ticker'
        for ticker in tick_chunk:
            try:
                df = data[ticker] if isinstance(data.columns, pd.MultiIndex) else data
                if df is None or df.empty:
                    continue
                # Ensure expected columns
                if "Close" not in df.columns or "Volume" not in df.columns:
                    continue
                company = _get_company_for_symbol(ticker)
                for date, row in df.iterrows():
                    close = row.get("Close")
                    vol = row.get("Volume")
                    if pd.isna(close) or pd.isna(vol):
                        continue
                    rows.append({
                        "symbol": ticker,
                        "date": date.to_pydatetime() if hasattr(date, 'to_pydatetime') else date,
                        "price": float(close),
                        "volume": int(vol),
                        "company": company,
                    })
            except Exception:
                continue
        inserted = _bulk_insert_rows(rows)
        with _PROGRESS_LOCK:
            _INGEST_PROGRESS["processed_tickers"] += len(tick_chunk)
            _INGEST_PROGRESS["rows_inserted"] += inserted
            _INGEST_PROGRESS["current_chunk"] = idx + 1
        return inserted, len(tick_chunk)

    if total_chunks == 0:
        _set_progress(done=True)
        return {"total": 0, "rows_inserted": 0, "skipped": len(all_tickers)}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for idx, tick_chunk in enumerate(chunks):
            futures.append(executor.submit(process_chunk, idx, tick_chunk))
        for f in futures:
            f.result()

    _set_progress(done=True)
    prog = get_ingest_progress()
    return {"total": prog["total_tickers"], "rows_inserted": prog["rows_inserted"], "processed": prog["processed_tickers"]}


def analyze_stock_from_db(ticker, min_volume_surge_pct: float = 1.5):
    """Compute metrics using only DB data; no API calls. Filter by surge threshold."""
    try:
        db = SessionLocal()
        rows = db.execute(text(
            """
            SELECT date, close_price, volume
            FROM stock_prices
            WHERE symbol = :symbol
            ORDER BY date ASC
            LIMIT 60
            """
        ), {"symbol": ticker}).fetchall()
        db.close()

        if not rows or len(rows) < 21:
            return None

        hist = pd.DataFrame(rows, columns=["Date", "Close", "Volume"]).sort_values("Date")

        volumes = hist["Volume"].dropna()
        prices = hist["Close"].dropna()
        if len(volumes) < 21 or len(prices) < 2:
            return None

        # Compute 20d avg using prior 20 days, then today's metrics
        avg_volume = volumes.iloc[-21:-1].mean()
        today_volume = volumes.iloc[-1]
        prev_close = prices.iloc[-2]
        price = prices.iloc[-1]
        if not pd.notna(avg_volume) or avg_volume <= 0:
            return None
        if not pd.notna(today_volume) or today_volume <= 0:
            return None
        if not pd.notna(price) or not pd.notna(prev_close) or prev_close == 0:
            return None

        volume_surge = ((today_volume - avg_volume) / avg_volume) * 100
        price_change = ((price - prev_close) / prev_close) * 100

        # Core surge threshold filter
        if not pd.notna(volume_surge) or volume_surge < min_volume_surge_pct:
            return None

        # Fetch company from latest row for this symbol if exists
        db = SessionLocal()
        comp_row = db.execute(text(
            "SELECT company FROM stock_prices WHERE symbol=:symbol AND company IS NOT NULL AND company<>'' ORDER BY date DESC LIMIT 1"
        ), {"symbol": ticker}).fetchone()
        db.close()
        company = comp_row[0] if comp_row and comp_row[0] else _get_company_for_symbol(ticker)

        return {
            "symbol": ticker,
            "company": company or "",
            "price": float(round(price, 2)),
            "price_change": float(round(price_change, 2)),
            "market_cap_billion": 0,  # filled later optionally
            "today_volume": int(today_volume),
            "avg_volume": int(avg_volume),
            "volume_surge": float(round(volume_surge, 2)),
        }
    except Exception:
        return None


def get_top_stocks_from_db(min_volume_surge_pct: float = 1.5, limit: int = 10):
    """Compute over ALL tickers with configurable surge threshold and return top ranked, applying liquidity and penny-stock filters."""
    all_tickers = load_tickers()

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda t: analyze_stock_from_db(t, min_volume_surge_pct), all_tickers))

    # Initial surge filter already applied; now apply liquidity and price filters
    filtered = []
    for r in results:
        if not r:
            continue
        # Filters: exclude avg_volume < 500k, price < 5
        if r["avg_volume"] < 500_000:
            continue
        if r["price"] < 5:
            continue
        filtered.append(r)

    # Enrich market cap and filter market cap < 1B
    for r in filtered:
        try:
            tk = yf.Ticker(r["symbol"]) 
            info = tk.info
            mc = info.get("marketCap")
            r["market_cap_billion"] = round(mc / 1_000_000_000, 2) if mc else 0
            # If company missing in payload, set from info
            if not r.get("company"):
                r["company"] = info.get("longName") or info.get("shortName") or info.get("name") or ""
        except Exception:
            r["market_cap_billion"] = 0

    filtered = [r for r in filtered if r.get("market_cap_billion", 0) >= 1]

    filtered.sort(key=lambda x: x["volume_surge"], reverse=True)

    top = filtered[:max(5, min(limit, len(filtered)))]
    return top


def backfill_company_missing():
    """Temporary utility: fill company for rows with NULL/empty company using yfinance info per symbol."""
    db = SessionLocal()
    try:
        symbols = db.execute(text(
            "SELECT DISTINCT symbol FROM stock_prices WHERE company IS NULL OR company=''"
        )).fetchall()
        symbols = [s[0] for s in symbols]
        updated = 0
        errors = []
        for sym in symbols:
            try:
                comp = _get_company_for_symbol(sym)
                if comp:
                    db.execute(text(
                        "UPDATE stock_prices SET company=:company WHERE symbol=:symbol AND (company IS NULL OR company='')"
                    ), {"company": comp, "symbol": sym})
                    updated += 1
            except Exception as e:
                errors.append({"symbol": sym, "error": str(e)})
        db.commit()
        return {"processed": len(symbols), "updated": updated, "errors": errors}
    finally:
        db.close()
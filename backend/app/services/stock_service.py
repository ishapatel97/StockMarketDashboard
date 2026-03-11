import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import List

from database import SessionLocal
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

# ── Polygon.io config ─────────────────────────────────────────────────────────
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
POLYGON_BASE    = "https://api.polygon.io"
BATCH_SIZE      = 200

# ── In-memory progress tracker ────────────────────────────────────────────────
_INGEST_PROGRESS = {
    "started": False, "done": False, "total_tickers": 0,
    "processed_tickers": 0, "rows_inserted": 0,
    "current_chunk": 0, "total_chunks": 0,
}
_PROGRESS_LOCK = Lock()


# ── Polygon helpers ───────────────────────────────────────────────────────────

def _polygon_get(path: str, params: dict = None) -> dict:
    url = f"{POLYGON_BASE}{path}"
    p = params or {}
    p["apiKey"] = POLYGON_API_KEY
    resp = requests.get(url, params=p, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _get_ticker_details(symbol: str) -> dict:
    """
    Fetch company name, market_cap, and sector from Polygon Ticker Details.
    Returns dict with keys: company, market_cap, sector.
    """
    try:
        data = _polygon_get(f"/v3/reference/tickers/{symbol}")
        results = data.get("results", {})
        return {
            "company":    results.get("name") or "",
            "market_cap": results.get("market_cap"),  # can be None
            "sector":     results.get("sic_description") or "",
        }
    except Exception:
        return {"company": "", "market_cap": None, "sector": ""}


def _get_company_for_symbol(symbol: str) -> str:
    """Legacy wrapper — returns just the company name."""
    return _get_ticker_details(symbol).get("company", "")


def _get_ohlcv(symbol: str, days: int = 60) -> pd.DataFrame:
    to_date   = datetime.today().strftime("%Y-%m-%d")
    from_date = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        data = _polygon_get(
            f"/v2/aggs/ticker/{symbol}/range/1/day/{from_date}/{to_date}",
            params={"adjusted": "true", "sort": "asc", "limit": 120},
        )
        results = data.get("results")
        if not results:
            return pd.DataFrame()
        rows = [{"Date": datetime.utcfromtimestamp(r["t"] / 1000).date(),
                 "Close": r.get("c"), "Volume": r.get("v")} for r in results]
        df = pd.DataFrame(rows)
        df["Date"]   = pd.to_datetime(df["Date"])
        df["Close"]  = pd.to_numeric(df["Close"],  errors="coerce")
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")
        return df.sort_values("Date").reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


# ── Ticker loader ─────────────────────────────────────────────────────────────

def load_tickers():
    for path in ["all_tickers.txt", "app/data/all_tickers.txt"]:
        if os.path.exists(path):
            tickers = []
            with open(path, "r") as f:
                for line in f:
                    ticker = line.strip()
                    if ticker and "^" not in ticker and "/" not in ticker and ticker.isalpha():
                        tickers.append(ticker)
            return tickers
    print("WARNING: all_tickers.txt not found!")
    return []


# ── DB helpers ────────────────────────────────────────────────────────────────

def _backfill_company_symbol(db, symbol: str, company: str):
    if not company:
        return
    db.execute(text(
        "UPDATE stock_prices SET company=:company WHERE symbol=:symbol AND (company IS NULL OR company='')"
    ), {"company": company, "symbol": symbol})


def _bulk_insert_rows(rows: list) -> int:
    """Insert price rows. Supports both old format (no market_cap/sector) and new format."""
    if not rows:
        return 0
    db = SessionLocal()
    try:
        # Check if rows have market_cap/sector (new format)
        has_metadata = "market_cap" in rows[0]
        if has_metadata:
            db.execute(text("""
                INSERT INTO stock_prices(symbol, date, close_price, volume, company, market_cap, sector)
                VALUES(:symbol, :date, :price, :volume, :company, :market_cap, :sector)
                ON CONFLICT(symbol, date) DO UPDATE SET
                    close_price = EXCLUDED.close_price,
                    volume      = EXCLUDED.volume,
                    company     = COALESCE(NULLIF(EXCLUDED.company, ''), stock_prices.company),
                    market_cap  = COALESCE(EXCLUDED.market_cap, stock_prices.market_cap),
                    sector      = COALESCE(NULLIF(stock_prices.sector, ''), EXCLUDED.sector)
            """), rows)
        else:
            db.execute(text("""
                INSERT INTO stock_prices(symbol, date, close_price, volume, company)
                VALUES(:symbol, :date, :price, :volume, :company)
                ON CONFLICT(symbol, date) DO NOTHING
            """), rows)
            symbols = {r["symbol"] for r in rows if r.get("company")}
            for sym in symbols:
                comp = next((r["company"] for r in rows if r["symbol"] == sym and r.get("company")), "")
                if comp:
                    _backfill_company_symbol(db, sym, comp)
        db.commit()
        return len(rows)
    except Exception as e:
        db.rollback()
        print(f"Bulk insert error: {e}")
        return 0
    finally:
        db.close()


def save_history(symbol: str, df: pd.DataFrame, company: str = ""):
    if df.empty:
        return
    db = SessionLocal()
    try:
        if not company:
            company = _get_company_for_symbol(symbol)
        for _, row in df.iterrows():
            db.execute(text("""
                INSERT INTO stock_prices(symbol, date, close_price, volume, company)
                VALUES(:symbol, :date, :price, :volume, :company)
                ON CONFLICT(symbol, date) DO NOTHING
            """), {
                "symbol":  symbol,
                "date":    row["Date"].to_pydatetime() if hasattr(row["Date"], "to_pydatetime") else row["Date"],
                "price":   float(row["Close"]) if pd.notna(row["Close"]) else None,
                "volume":  int(row["Volume"])  if pd.notna(row["Volume"]) else None,
                "company": company,
            })
        _backfill_company_symbol(db, symbol, company)
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        raise
    finally:
        db.close()


# ── Progress helpers ──────────────────────────────────────────────────────────

def _set_progress(**kwargs):
    with _PROGRESS_LOCK:
        _INGEST_PROGRESS.update(kwargs)


def get_ingest_progress():
    with _PROGRESS_LOCK:
        return dict(_INGEST_PROGRESS)


def _chunk_list(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


# ── Ingestion ─────────────────────────────────────────────────────────────────

def ingest_all_tickers_fast(chunk_size: int = 50, max_workers: int = 5):
    all_tickers = load_tickers()
    print(f"Loaded {len(all_tickers)} tickers")

    db = SessionLocal()
    counts = db.execute(text("SELECT symbol, COUNT(*) FROM stock_prices GROUP BY symbol")).fetchall()
    db.close()
    have_enough = {sym for sym, c in counts if (c or 0) >= 20}
    remaining   = [t for t in all_tickers if t not in have_enough]

    total        = len(remaining)
    chunks       = list(_chunk_list(remaining, chunk_size))
    total_chunks = len(chunks)

    print(f"Ingesting {total} tickers in {total_chunks} chunks")
    _set_progress(started=True, done=False, total_tickers=total,
                  processed_tickers=0, rows_inserted=0,
                  current_chunk=0, total_chunks=total_chunks)

    def process_ticker(ticker: str) -> list:
        try:
            company = _get_company_for_symbol(ticker)
            df      = _get_ohlcv(ticker, days=60)
            if df.empty:
                return []
            rows = []
            for _, row in df.iterrows():
                if pd.isna(row["Close"]) or pd.isna(row["Volume"]):
                    continue
                rows.append({
                    "symbol":  ticker,
                    "date":    row["Date"].to_pydatetime() if hasattr(row["Date"], "to_pydatetime") else row["Date"],
                    "price":   float(row["Close"]),
                    "volume":  int(row["Volume"]),
                    "company": company,
                })
            return rows
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
            return []

    def process_chunk(idx: int, tick_chunk: list):
        all_rows = []
        for ticker in tick_chunk:
            all_rows.extend(process_ticker(ticker))
            time.sleep(0.2)
        inserted = _bulk_insert_rows(all_rows)
        with _PROGRESS_LOCK:
            _INGEST_PROGRESS["processed_tickers"] += len(tick_chunk)
            _INGEST_PROGRESS["rows_inserted"]      += inserted
            _INGEST_PROGRESS["current_chunk"]       = idx + 1
        print(f"Chunk {idx+1}/{total_chunks} done — inserted {inserted} rows")
        return inserted

    if total_chunks == 0:
        _set_progress(done=True)
        return {"total": 0, "rows_inserted": 0, "skipped": len(all_tickers)}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_chunk, idx, chunk) for idx, chunk in enumerate(chunks)]
        for f in futures:
            f.result()

    _set_progress(done=True)
    prog = get_ingest_progress()
    return {"total": prog["total_tickers"], "rows_inserted": prog["rows_inserted"], "processed": prog["processed_tickers"]}


def ingest_latest_prices(max_workers: int = 3):
    """
    Lightweight daily ingest — fetches only last 2 days of price data
    + company/market_cap/sector from Polygon for ALL tickers already in DB.
    Runs every 4 hours + market open. Much faster than full 60-day ingest.
    After completion, auto-triggers refresh_calculated_stocks().
    """
    print("ingest_latest_prices: starting...")

    # Get all symbols that already have data in DB
    db = SessionLocal()
    try:
        rows = db.execute(text(
            "SELECT DISTINCT symbol FROM stock_prices"
        )).fetchall()
        all_symbols = [r[0] for r in rows]
    finally:
        db.close()

    if not all_symbols:
        print("ingest_latest_prices: no symbols in DB, skipping")
        return {"total": 0, "rows_inserted": 0}

    print(f"ingest_latest_prices: updating {len(all_symbols)} symbols")

    def process_ticker(ticker: str) -> list:
        try:
            # 1) Get metadata (company, market_cap, sector) — 1 API call
            details = _get_ticker_details(ticker)

            # 2) Get last 5 days of prices — 1 API call (5 days to cover weekends)
            df = _get_ohlcv(ticker, days=5)
            if df.empty:
                return []

            rows = []
            for _, row in df.iterrows():
                if pd.isna(row["Close"]) or pd.isna(row["Volume"]):
                    continue
                rows.append({
                    "symbol":     ticker,
                    "date":       row["Date"].to_pydatetime() if hasattr(row["Date"], "to_pydatetime") else row["Date"],
                    "price":      float(row["Close"]),
                    "volume":     int(row["Volume"]),
                    "company":    details["company"],
                    "market_cap": details["market_cap"],
                    "sector":     details["sector"],
                })
            return rows
        except Exception as e:
            print(f"  ingest_latest error [{ticker}]: {e}")
            return []

    # Process in chunks to respect Polygon rate limits (5 calls/min on free tier)
    # Each ticker = 2 API calls (details + ohlcv), so ~2.5 tickers/min on free tier
    # With sleep(0.5) between tickers = safe margin
    chunks = list(_chunk_list(all_symbols, 50))
    total_inserted = 0

    for chunk_idx, chunk in enumerate(chunks):
        all_rows = []
        for ticker in chunk:
            all_rows.extend(process_ticker(ticker))
            time.sleep(0.5)  # Polygon free tier rate limit

        inserted = _bulk_insert_rows(all_rows)
        total_inserted += inserted
        print(f"  Chunk {chunk_idx + 1}/{len(chunks)}: {inserted} rows upserted (total: {total_inserted})")

    print(f"ingest_latest_prices: DONE — {total_inserted} rows upserted for {len(all_symbols)} symbols")

    # Auto-trigger calculated_stocks refresh after new data is in
    print("ingest_latest_prices: triggering refresh_calculated_stocks...")
    refresh_calculated_stocks()

    return {"total": len(all_symbols), "rows_inserted": total_inserted}


def ingest_next_batch():
    all_tickers = load_tickers()
    db = SessionLocal()
    counts = db.execute(text("SELECT symbol, COUNT(*) FROM stock_prices GROUP BY symbol")).fetchall()
    db.close()
    have_enough = {sym for sym, c in counts if (c or 0) >= 20}
    remaining   = [t for t in all_tickers if t not in have_enough]
    batch       = remaining[:BATCH_SIZE]

    processed = []
    for ticker in batch:
        try:
            df      = _get_ohlcv(ticker, days=60)
            company = _get_company_for_symbol(ticker)
            if not df.empty:
                save_history(ticker, df, company)
                processed.append({"symbol": ticker, "ingested": True})
            else:
                processed.append({"symbol": ticker, "ingested": False})
        except Exception as e:
            processed.append({"symbol": ticker, "ingested": False, "error": str(e)})
        time.sleep(0.2)
    return {"batch_size": len(batch), "processed": processed}


# ── Sectors ───────────────────────────────────────────────────────────────────

def get_all_sectors() -> List[str]:
    """Return distinct non-null sectors from stock_prices."""
    db = SessionLocal()
    try:
        rows = db.execute(text("""
            SELECT DISTINCT sector FROM stock_prices
            WHERE sector IS NOT NULL AND sector != ''
            ORDER BY sector
        """)).fetchall()
        return [r[0] for r in rows]
    finally:
        db.close()


# ── Refresh calculated_stocks (every 10 hours) ───────────────────────────────

def refresh_calculated_stocks():
    """
    Heavy calculation runs here in the background every 10 hours.
    Reads stock_prices → calculates avg_volume_20d, volume_surge, price_change
    → calls Groq for 7-day stock_insight
    → upserts into calculated_stocks table (one row per symbol).
    """
    print("refresh_calculated_stocks: starting...")

    db = SessionLocal()
    try:
        rows = db.execute(text("""
            WITH ranked AS (
                -- Rank rows per symbol newest first, only valid rows
                SELECT
                    symbol, close_price, volume,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
                FROM stock_prices
                WHERE close_price IS NOT NULL
                  AND volume      IS NOT NULL
                  AND volume      > 0
            ),
            latest AS (
                -- rn=1 = most recent trading day = "today"
                SELECT symbol, close_price AS price, volume AS today_volume
                FROM ranked WHERE rn = 1
            ),
            prev AS (
                -- rn=2 = previous trading day = for price_change %
                SELECT symbol, close_price AS prev_close
                FROM ranked WHERE rn = 2
            ),
            avg_vol AS (
                -- rn=2 to rn=21 = up to 20 previous days for avg
                -- HAVING >= 10 allows symbols with at least 10 days history
                -- (avoids rejecting valid symbols that have slightly less than 20 days)
                SELECT
                    symbol,
                    ROUND(AVG(volume::NUMERIC), 0)::BIGINT AS avg_volume_20d
                FROM ranked
                WHERE rn BETWEEN 2 AND 21
                GROUP BY symbol
                HAVING COUNT(*) >= 10
            ),
            mc AS (
                SELECT DISTINCT ON (symbol) symbol, market_cap
                FROM stock_prices
                WHERE market_cap IS NOT NULL
                ORDER BY symbol, date DESC
            ),
            sec AS (
                SELECT DISTINCT ON (symbol) symbol, sector
                FROM stock_prices
                WHERE sector IS NOT NULL AND sector != ''
                ORDER BY symbol, date DESC
            )
            SELECT
                l.symbol,
                a.avg_volume_20d,
                ROUND(
                    ((l.today_volume::NUMERIC - a.avg_volume_20d::NUMERIC)
                     / NULLIF(a.avg_volume_20d::NUMERIC, 0) * 100),
                2) AS volume_surge,
                ROUND(
                    ((l.price::NUMERIC - p.prev_close::NUMERIC)
                     / NULLIF(p.prev_close::NUMERIC, 0) * 100),
                2) AS price_change,
                l.price,
                mc.market_cap,
                sec.sector
            FROM latest  l
            JOIN prev    p  ON l.symbol = p.symbol
            JOIN avg_vol a  ON l.symbol = a.symbol
            JOIN mc         ON l.symbol = mc.symbol
            JOIN sec ON l.symbol = sec.symbol
            WHERE a.avg_volume_20d >= 500000
              AND l.price          >= 5
              AND mc.market_cap    >= 1000000000
        """)).fetchall()
    except Exception as e:
        print(f"refresh_calculated_stocks SQL error: {e}")
        db.close()
        return
    finally:
        db.close()

    print(f"refresh_calculated_stocks: {len(rows)} symbols qualify for calculation")

    # Bulk upsert in batches of 100 (much faster than one-by-one)
    upsert_rows = []
    for r in rows:
        upsert_rows.append({
            "symbol":         r[0],
            "avg_volume_20d": int(r[1]),
            "volume_surge":   float(r[2]),
            "price_change":   float(r[3]),
            "sector":         str(r[6]) if r[6] else "", 
        })

    updated = 0
    batch_size = 100
    for i in range(0, len(upsert_rows), batch_size):
        batch = upsert_rows[i:i + batch_size]
        db2 = SessionLocal()
        try:
            db2.execute(text("""
                INSERT INTO calculated_stocks
                    (symbol, avg_volume_20d, volume_surge, price_change, sector, last_updated)
                VALUES
                    (:symbol, :avg_volume_20d, :volume_surge, :price_change, :sector, NOW())
                ON CONFLICT (symbol) DO UPDATE SET
                    avg_volume_20d = EXCLUDED.avg_volume_20d,
                    volume_surge   = EXCLUDED.volume_surge,
                    price_change   = EXCLUDED.price_change,
                    sector = EXCLUDED.sector,
                    last_updated   = NOW()
            """), batch)
            db2.commit()
            updated += len(batch)
            print(f"  Batch {i // batch_size + 1}: upserted {len(batch)} rows (total: {updated})")
        except Exception as e:
            db2.rollback()
            print(f"  Batch upsert error: {e}")
        finally:
            db2.close()

    print(f"refresh_calculated_stocks: DONE — {updated} symbols updated")


# ── Fast query from calculated_stocks ────────────────────────────────────────

def get_top_stocks_from_db(
    min_volume_surge_pct: float = 1.5,
    limit: int = 50,
    sectors: List[str] = None,
):
    """
    Instant query — reads pre-calculated data from calculated_stocks
    JOINed with latest row per symbol from stock_prices for
    current price, company, market_cap, sector.

    Optimized: first filters calculated_stocks (small table), then
    only joins the matching symbols from stock_prices via LATERAL join.
    """
    db = SessionLocal()
    try:
        sector_clause = ""
        params = {"threshold": min_volume_surge_pct, "limit": limit}

        if sectors:
            placeholders = ", ".join([f":sector_{i}" for i in range(len(sectors))])
            sector_clause = f"AND c.sector IN ({placeholders})"
            for i, s in enumerate(sectors):
                params[f"sector_{i}"] = s

        rows = db.execute(text(f"""
            SELECT
                c.symbol,
                sp.company,
                sp.sector,
                sp.close_price        AS price,
                sp.market_cap,
                sp.volume             AS today_volume,
                c.avg_volume_20d,
                c.volume_surge,
                c.price_change,
                c.stock_insight,
                c.last_updated
            FROM calculated_stocks c
            JOIN LATERAL (
                SELECT close_price, volume, company, market_cap, sector
                FROM stock_prices
                WHERE symbol = c.symbol
                ORDER BY date DESC
                LIMIT 1
            ) sp ON true
            WHERE c.volume_surge    >= :threshold
              AND c.avg_volume_20d  >= 500000
              AND sp.market_cap     >= 1000000000
              AND sp.close_price    >= 5
              {sector_clause}
            ORDER BY c.volume_surge DESC
            LIMIT :limit
        """), params).fetchall()
    except Exception as e:
        print(f"Error in get_top_stocks_from_db: {e}")
        return []
    finally:
        db.close()

    results = []
    for r in rows:
        try:
            mc = r[4]
            results.append({
                "symbol":             str(r[0]),
                "company":            str(r[1]) if r[1] else "",
                "sector":             str(r[2]) if r[2] else "",
                "price":              float(r[3]),
                "market_cap_billion": round(float(mc) / 1_000_000_000, 2) if mc else 0.0,
                "today_volume":       int(r[5]),
                "avg_volume":         int(r[6]),
                "volume_surge":       float(r[7]),
                "price_change":       float(r[8]),
                "stock_insight":      str(r[9]) if r[9] else "",
                "last_updated":       str(r[10])[:16] if r[10] else "",
            })
        except Exception as e:
            print(f"Row parse error {r[0]}: {e}")
            continue

    return results


# ── Chart data (from stock_prices directly) ───────────────────────────────────

def get_chart_data(symbol: str):
    db = SessionLocal()
    rows = db.execute(text("""
        SELECT date, close_price, volume FROM stock_prices
        WHERE symbol = :symbol ORDER BY date DESC LIMIT 20
    """), {"symbol": symbol}).fetchall()
    db.close()

    rows = list(reversed(rows))

    if len(rows) >= 20:
        dates   = [str(r[0])[:10] for r in rows]
        prices  = [float(r[1]) if r[1] else 0 for r in rows]
        volumes = [int(r[2])   if r[2] else 0 for r in rows]
    else:
        df      = _get_ohlcv(symbol, days=60)
        dates   = [str(d)[:10] for d in df["Date"]]
        prices  = df["Close"].fillna(0).tolist()
        volumes = df["Volume"].fillna(0).astype(int).tolist()

    return {"symbol": symbol, "dates": dates, "prices": prices, "volumes": volumes}
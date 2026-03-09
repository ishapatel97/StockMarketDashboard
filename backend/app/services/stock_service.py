import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from database import SessionLocal
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

# ── Polygon.io config ─────────────────────────────────────────────────────────
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
POLYGON_BASE    = "https://api.polygon.io"
BATCH_SIZE      = 200

# ── In-memory progress tracker ────────────────────────────────────────────────
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


# ── Polygon helpers ───────────────────────────────────────────────────────────

def _polygon_get(path: str, params: dict = None) -> dict:
    url = f"{POLYGON_BASE}{path}"
    p = params or {}
    p["apiKey"] = POLYGON_API_KEY
    resp = requests.get(url, params=p, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _get_company_for_symbol(symbol: str) -> str:
    try:
        data = _polygon_get(f"/v3/reference/tickers/{symbol}")
        return data.get("results", {}).get("name", "")
    except Exception:
        return ""


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
    if not rows:
        return 0
    db = SessionLocal()
    try:
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


# ── Analysis ──────────────────────────────────────────────────────────────────

def analyze_stock_from_db(ticker: str, min_volume_surge_pct: float = 1.5):
    try:
        db = SessionLocal()
        rows = db.execute(text("""
            SELECT date, close_price, volume FROM stock_prices
            WHERE symbol = :symbol ORDER BY date ASC LIMIT 60
        """), {"symbol": ticker}).fetchall()
        db.close()

        if not rows or len(rows) < 21:
            return None

        hist    = pd.DataFrame(rows, columns=["Date", "Close", "Volume"]).sort_values("Date")
        volumes = hist["Volume"].dropna()
        prices  = hist["Close"].dropna()

        if len(volumes) < 21 or len(prices) < 2:
            return None

        avg_volume   = volumes.iloc[-21:-1].mean()
        today_volume = volumes.iloc[-1]
        prev_close   = prices.iloc[-2]
        price        = prices.iloc[-1]

        if not all(pd.notna(x) for x in [avg_volume, today_volume, price, prev_close]):
            return None
        if avg_volume <= 0 or today_volume <= 0 or prev_close == 0:
            return None

        volume_surge = ((today_volume - avg_volume) / avg_volume) * 100
        price_change = ((price - prev_close) / prev_close) * 100

        if volume_surge < min_volume_surge_pct:
            return None

        db = SessionLocal()
        comp_row = db.execute(text(
            "SELECT company, market_cap FROM stock_prices WHERE symbol=:symbol AND company IS NOT NULL AND company<>'' ORDER BY date DESC LIMIT 1"
        ), {"symbol": ticker}).fetchone()
        db.close()

        mc = comp_row[1] if comp_row and comp_row[1] else None

        return {
            "symbol":             ticker,
            "company":            comp_row[0] if comp_row else "",
            "price":              float(round(price, 2)),
            "price_change":       float(round(price_change, 2)),
            "market_cap_billion": round(float(mc) / 1_000_000_000, 2) if mc else 0.0,
            "today_volume":       int(today_volume),
            "avg_volume":         int(avg_volume),
            "volume_surge":       float(round(volume_surge, 2)),
        }
    except Exception:
        return None


def get_top_stocks_from_db(min_volume_surge_pct: float = 1.5, limit: int = 10):
    db = SessionLocal()
    try:
        rows = db.execute(text("""
            WITH ranked AS (
                SELECT
                    symbol, company, close_price, volume,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn,
                    COUNT(*)     OVER (PARTITION BY symbol)                    AS total_rows
                FROM stock_prices
                WHERE close_price IS NOT NULL AND volume IS NOT NULL
            ),
            latest AS (
                SELECT symbol, company, close_price AS price, volume AS today_volume
                FROM ranked WHERE rn = 1 AND total_rows >= 21
            ),
            prev AS (
                SELECT symbol, close_price AS prev_close
                FROM ranked WHERE rn = 2
            ),
            avg_vol AS (
                SELECT symbol, AVG(volume) AS avg_volume
                FROM ranked WHERE rn BETWEEN 2 AND 21
                GROUP BY symbol
            ),
            mc AS (
                SELECT DISTINCT ON (symbol) symbol, market_cap
                FROM stock_prices
                WHERE market_cap IS NOT NULL
                ORDER BY symbol, date DESC
            )
            SELECT
                l.symbol,
                l.company,
                ROUND(l.price::numeric, 2)                                                           AS price,
                l.today_volume,
                ROUND(a.avg_volume::numeric, 0)                                                      AS avg_volume,
                ROUND(((l.today_volume - a.avg_volume) / NULLIF(a.avg_volume,0) * 100)::numeric, 2) AS volume_surge,
                ROUND(((l.price - p.prev_close) / NULLIF(p.prev_close,0) * 100)::numeric, 2)        AS price_change,
                mc.market_cap
            FROM latest l
            JOIN prev    p  ON l.symbol = p.symbol
            JOIN avg_vol a  ON l.symbol = a.symbol
            JOIN mc    ON l.symbol = mc.symbol
            WHERE a.avg_volume >= 500000
              AND l.price >= 5
              AND ((l.today_volume - a.avg_volume) / NULLIF(a.avg_volume,0) * 100) >= :threshold
              AND mc.market_cap >= 1000000000
            ORDER BY volume_surge DESC
            LIMIT :limit
        """), {"threshold": min_volume_surge_pct, "limit": limit}).fetchall()
    except Exception as e:
        print(f"Error in get_top_stocks_from_db: {e}")
        return []
    finally:
        db.close()

    results = []
    for r in rows:
        try:
            mc = r[7]
            results.append({
                "symbol":             str(r[0]),
                "company":            str(r[1]) if r[1] else "",
                "price":              float(r[2]),
                "price_change":       float(r[6]),
                "market_cap_billion": round(float(mc) / 1_000_000_000, 2) if mc else 0.0,
                "today_volume":       int(float(r[3])),
                "avg_volume":         int(float(r[4])),
                "volume_surge":       float(r[5]),
            })
        except Exception as e:
            print(f"Row parse error {r[0]}: {e}")
            continue

    return results


def get_chart_data(symbol: str):
    db = SessionLocal()
    rows = db.execute(text("""
        SELECT date, close_price, volume FROM stock_prices
        WHERE symbol = :symbol ORDER BY date DESC LIMIT 20
    """), {"symbol": symbol}).fetchall()
    db.close()

# Reverse to get ascending order for chart
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
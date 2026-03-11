"""
backfill_metadata_yf.py
-----------------------
Standalone script to fill NULL market_cap and sector values in stock_prices.
Uses yfinance to fetch company details for each symbol missing metadata.

Run locally: python backfill_metadata_yf.py
(yfinance is blocked on Render — run this locally only)
"""

import time
import yfinance as yf
from sqlalchemy import create_engine, text

# ── Config ────────────────────────────────────────────────────────────────────

DATABASE_URL  = "postgresql://stockmarket_db_6z4q_user:App1I3cu54KCLdVlYKalVFZlIr4v9uvO@dpg-d6n3u8450q8c73atq74g-a.virginia-postgres.render.com/stockmarket_db_6z4q"
SLEEP_BETWEEN = 0.3  # seconds between yfinance calls

engine = create_engine(DATABASE_URL)

# ── yfinance helper ───────────────────────────────────────────────────────────

def get_ticker_details(symbol: str) -> dict:
    """Fetch market_cap and sector from yfinance."""
    try:
        info = yf.Ticker(symbol).info
        return {
            "company":    info.get("longName") or info.get("shortName") or "",
            "market_cap": info.get("marketCap"),      # can be None
            "sector":     info.get("sector") or "",   # clean: "Healthcare", "Technology" etc.
        }
    except Exception as e:
        print(f"  yfinance error [{symbol}]: {e}")
        return {"company": "", "market_cap": None, "sector": ""}


# ── Main ──────────────────────────────────────────────────────────────────────

def backfill_metadata():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT symbol
            FROM stock_prices
            WHERE market_cap IS NULL
               OR sector IS NULL
               OR sector = ''
            ORDER BY symbol
        """)).fetchall()

    symbols = [r[0] for r in rows]
    total   = len(symbols)
    print(f"Found {total} symbols with missing market_cap or sector.")

    if total == 0:
        print("Nothing to backfill. Exiting.")
        return

    updated = 0
    skipped = 0

    for i, symbol in enumerate(symbols, 1):
        print(f"[{i}/{total}] {symbol} ...", end=" ")

        details = get_ticker_details(symbol)

        if not details["market_cap"] and not details["sector"]:
            print("no data from yfinance, skipping.")
            skipped += 1
            time.sleep(SLEEP_BETWEEN)
            continue

        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE stock_prices
                SET
                    market_cap = COALESCE(market_cap, :market_cap),
                    sector     = CASE
                                    WHEN sector IS NULL OR sector = ''
                                    THEN COALESCE(NULLIF(:sector, ''), sector)
                                    ELSE sector
                                 END,
                    company    = CASE
                                    WHEN company IS NULL OR company = ''
                                    THEN COALESCE(NULLIF(:company, ''), company)
                                    ELSE company
                                 END
                WHERE symbol = :symbol
            """), {
                "symbol":     symbol,
                "market_cap": details["market_cap"],
                "sector":     details["sector"] or None,
                "company":    details["company"] or None,
            })
            conn.commit()

        updated += 1
        print(f"updated — market_cap={details['market_cap']}, sector={details['sector']}")
        time.sleep(SLEEP_BETWEEN)

    print(f"\nDone. Updated: {updated} | Skipped (no yfinance data): {skipped} | Total: {total}")


if __name__ == "__main__":
    backfill_metadata()
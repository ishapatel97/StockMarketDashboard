"""
Simple standalone script to backfill company name and market cap
using yfinance. Run this locally from your backend folder:

    python backfill_yfinance.py

It only processes symbols still missing company OR market_cap.
Safe to stop and restart — resumes automatically.
"""

import os
import time
import yfinance as yf
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ── DB connection ─────────────────────────────────────────────────────────────
DATABASE_URL = "postgresql://stockmarket_db_6z4q_user:App1I3cu54KCLdVlYKalVFZlIr4v9uvO@dpg-d6n3u8450q8c73atq74g-a.virginia-postgres.render.com/stockmarket_db_6z4q"

engine       = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

# ── Fetch from DB: only symbols missing company OR market_cap ─────────────────
def get_symbols_to_fill():
    db = SessionLocal()
    rows = db.execute(text("""
        SELECT DISTINCT symbol FROM stock_prices
        WHERE (company IS NULL OR company = '')
           OR market_cap IS NULL
        ORDER BY symbol
    """)).fetchall()
    db.close()
    return [r[0] for r in rows]


# ── Update DB for one symbol ──────────────────────────────────────────────────
def update_symbol(symbol: str, company: str, market_cap: int):
    db = SessionLocal()
    try:
        if company:
            db.execute(text(
                "UPDATE stock_prices SET company=:company WHERE symbol=:symbol AND (company IS NULL OR company='')"
            ), {"company": company, "symbol": symbol})

        if market_cap:
            db.execute(text(
                "UPDATE stock_prices SET market_cap=:mc WHERE symbol=:symbol AND market_cap IS NULL"
            ), {"mc": market_cap, "symbol": symbol})

        db.commit()
    except Exception as e:
        db.rollback()
        print(f"  DB error for {symbol}: {e}")
    finally:
        db.close()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    symbols = get_symbols_to_fill()
    total   = len(symbols)
    print(f"Found {total} symbols missing company/market_cap\n")

    updated_company   = 0
    updated_marketcap = 0
    skipped           = 0

    for idx, symbol in enumerate(symbols, start=1):
        try:
            info       = yf.Ticker(symbol).info
            company    = info.get("longName") or info.get("shortName") or ""
            market_cap = info.get("marketCap")

            update_symbol(symbol, company, market_cap)

            if company:
                updated_company += 1
            if market_cap:
                updated_marketcap += 1
            if not company and not market_cap:
                skipped += 1

            print(f"[{idx}/{total}] {symbol} | company='{company}' | mc={market_cap}")

        except Exception as e:
            skipped += 1
            print(f"[{idx}/{total}] {symbol} | ERROR: {e}")

        # Small delay to avoid yfinance rate limiting
        time.sleep(0.3)

        # Print summary every 50 symbols
        if idx % 50 == 0:
            print(f"\n--- Progress: {idx}/{total} | company={updated_company} | marketcap={updated_marketcap} | skipped={skipped} ---\n")

    print(f"\nDone! company={updated_company} | marketcap={updated_marketcap} | skipped={skipped}")


if __name__ == "__main__":
    main()
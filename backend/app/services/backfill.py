"""
Standalone backfill script using yfinance.
Fills missing: company name, market cap, AND sector.

Run from your backend folder:
    python backfill_yfinance.py

Safe to stop and restart — resumes automatically (skips already filled data).
"""

import time
import yfinance as yf
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ── DB connection ─────────────────────────────────────────────────────────────
DATABASE_URL = "postgresql://stockmarket_db_6z4q_user:App1I3cu54KCLdVlYKalVFZlIr4v9uvO@dpg-d6n3u8450q8c73atq74g-a.virginia-postgres.render.com/stockmarket_db_6z4q"

engine       = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


# ── Fetch symbols that are missing ANY of the three fields ────────────────────
def get_symbols_to_fill():
    db = SessionLocal()
    rows = db.execute(text("""
        SELECT DISTINCT symbol FROM stock_prices
        WHERE (company IS NULL OR company = '')
           OR market_cap IS NULL
           OR sector IS NULL OR sector = ''
        ORDER BY symbol
    """)).fetchall()
    db.close()
    return [r[0] for r in rows]


# ── Update DB for one symbol ──────────────────────────────────────────────────
def update_symbol(symbol: str, company: str, market_cap: int, sector: str):
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

        if sector:
            db.execute(text(
                "UPDATE stock_prices SET sector=:sector WHERE symbol=:symbol AND (sector IS NULL OR sector='')"
            ), {"sector": sector, "symbol": symbol})

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
    print(f"Found {total} symbols missing company/market_cap/sector\n")

    updated_company   = 0
    updated_marketcap = 0
    updated_sector    = 0
    skipped           = 0

    for idx, symbol in enumerate(symbols, start=1):
        try:
            info       = yf.Ticker(symbol).info
            company    = info.get("longName") or info.get("shortName") or ""
            market_cap = info.get("marketCap")
            sector     = info.get("sector") or ""

            update_symbol(symbol, company, market_cap, sector)

            if company:    updated_company   += 1
            if market_cap: updated_marketcap += 1
            if sector:     updated_sector    += 1
            if not company and not market_cap and not sector:
                skipped += 1

            print(f"[{idx}/{total}] {symbol} | company='{company}' | mc={market_cap} | sector='{sector}'")

        except Exception as e:
            skipped += 1
            print(f"[{idx}/{total}] {symbol} | ERROR: {e}")

        time.sleep(0.3)

        if idx % 50 == 0:
            print(f"\n--- Progress: {idx}/{total} | company={updated_company} | marketcap={updated_marketcap} | sector={updated_sector} | skipped={skipped} ---\n")

    print(f"\nDone! company={updated_company} | marketcap={updated_marketcap} | sector={updated_sector} | skipped={skipped}")


if __name__ == "__main__":
    main()
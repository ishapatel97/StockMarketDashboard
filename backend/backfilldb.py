"""
Fast full backfill using yfinance.
Inserts ALL fields: symbol, date, close_price, volume, company, market_cap, sector.
Forces overwrite of all fields on conflict.

Run from your backend folder:
    python backfill_full.py
"""

import time
import yfinance as yf
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ── DB ────────────────────────────────────────────────────────────────────────
DATABASE_URL = "postgresql://stockmarket_db_6z4q_user:App1I3cu54KCLdVlYKalVFZlIr4v9uvO@dpg-d6n3u8450q8c73atq74g-a.virginia-postgres.render.com/stockmarket_db_6z4q"
engine       = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


# ── Load tickers ──────────────────────────────────────────────────────────────
def load_tickers():
    for path in ["app/data/all_tickers.txt", "all_tickers.txt"]:
        try:
            tickers = []
            with open(path) as f:
                for line in f:
                    t = line.strip()
                    if t and "^" not in t and "/" not in t and t.isalpha():
                        tickers.append(t)
            print(f"Loaded {len(tickers)} tickers from {path}")
            return tickers
        except FileNotFoundError:
            continue
    print("ERROR: all_tickers.txt not found!")
    return []


# ── Get already-complete symbols (skip them) ──────────────────────────────────
def get_complete_symbols():
    db = SessionLocal()
    try:
        rows = db.execute(text("""
            SELECT symbol FROM stock_prices
            GROUP BY symbol
            HAVING COUNT(*) >= 20
               AND MAX(CASE WHEN company    IS NOT NULL AND company    != '' THEN 1 ELSE 0 END) = 1
               AND MAX(CASE WHEN market_cap IS NOT NULL                      THEN 1 ELSE 0 END) = 1
               AND MAX(CASE WHEN sector     IS NOT NULL AND sector     != '' THEN 1 ELSE 0 END) = 1
        """)).fetchall()
        return {r[0] for r in rows}
    finally:
        db.close()


# ── Fetch everything for one symbol from yfinance ────────────────────────────
def fetch_symbol(symbol: str):
    try:
        ticker = yf.Ticker(symbol)

        # 60 days of daily OHLCV
        hist = ticker.history(period="3mo", interval="1d", auto_adjust=True)
        if hist.empty:
            return []

        # Metadata
        company    = ""
        market_cap = None
        sector     = ""
        try:
            info       = ticker.info
            company    = info.get("longName") or info.get("shortName") or ""
            market_cap = info.get("marketCap")
            sector     = info.get("sector") or ""
        except Exception:
            pass

        rows = []
        for idx, row in hist.iterrows():
            try:
                close  = float(row["Close"])
                volume = int(row["Volume"])
                if close != close or volume != volume:  # NaN check
                    continue
                rows.append({
                    "symbol":      symbol,
                    "date":        idx.date(),
                    "close_price": close,
                    "volume":      volume,
                    "company":     company,
                    "market_cap":  market_cap,
                    "sector":      sector,
                })
            except Exception:
                continue

        return rows

    except Exception as e:
        print(f"  yfinance error [{symbol}]: {e}")
        return []


# ── Upsert rows — force overwrite all fields ──────────────────────────────────
def upsert_rows(rows: list) -> int:
    if not rows:
        return 0
    db = SessionLocal()
    try:
        db.execute(text("""
            INSERT INTO stock_prices (symbol, date, close_price, volume, company, market_cap, sector)
            VALUES (:symbol, :date, :close_price, :volume, :company, :market_cap, :sector)
            ON CONFLICT (symbol, date) DO UPDATE SET
                close_price = EXCLUDED.close_price,
                volume      = EXCLUDED.volume,
                company     = EXCLUDED.company,
                market_cap  = EXCLUDED.market_cap,
                sector      = EXCLUDED.sector
        """), rows)
        db.commit()
        return len(rows)
    except Exception as e:
        db.rollback()
        print(f"  DB error: {e}")
        return 0
    finally:
        db.close()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    all_tickers      = load_tickers()
    complete_symbols = get_complete_symbols()
    to_process       = [t for t in all_tickers if t not in complete_symbols]
    total            = len(to_process)

    print(f"\nTotal tickers     : {len(all_tickers)}")
    print(f"Already complete  : {len(complete_symbols)}")
    print(f"To process        : {total}")
    print(f"{'─'*70}\n")

    total_rows = 0
    no_data    = 0
    errors     = 0

    for idx, symbol in enumerate(to_process, start=1):
        rows = fetch_symbol(symbol)

        if not rows:
            no_data += 1
            print(f"[{idx:>5}/{total}] {symbol:<8} | NO DATA")
        else:
            count = upsert_rows(rows)
            if count:
                total_rows += count
                r       = rows[0]
                mc_str  = f"${r['market_cap']/1e9:.1f}B" if r['market_cap'] else "no mc"
                sec_str = r['sector']   or "no sector"
                co_str  = r['company']  or "no company"
                # Show sample row: first date and last date
                dates   = [r['date'] for r in rows]
                print(f"[{idx:>5}/{total}] {symbol:<8} | {count:>3} rows ({min(dates)} → {max(dates)}) | {co_str[:25]:<25} | {mc_str:<10} | {sec_str}")
            else:
                errors += 1
                print(f"[{idx:>5}/{total}] {symbol:<8} | DB ERROR")

        time.sleep(0.3)  # yfinance rate limit buffer

        if idx % 50 == 0:
            print(f"\n{'─'*70}")
            print(f"  Progress : {idx}/{total} ({idx/total*100:.1f}%)")
            print(f"  Rows     : {total_rows:,}")
            print(f"  No data  : {no_data}  |  Errors: {errors}")
            print(f"{'─'*70}\n")

    print(f"\n{'═'*70}")
    print(f"  DONE!")
    print(f"  Rows inserted/updated : {total_rows:,}")
    print(f"  No data               : {no_data}")
    print(f"  DB errors             : {errors}")
    print(f"  Already complete      : {len(complete_symbols)}")
    print(f"{'═'*70}")


if __name__ == "__main__":
    main()
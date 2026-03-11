"""
refresh_calculated_stocks.py
----------------------------
Standalone script to recalculate and update the calculated_stocks table.
Reads from stock_prices → calculates avg_volume_20d, volume_surge, price_change, sector
→ upserts into calculated_stocks.

Run: python refresh_calculated_stocks.py
"""

from sqlalchemy import create_engine, text

# ── Config ────────────────────────────────────────────────────────────────────

DATABASE_URL = "postgresql://stockmarket_db_6z4q_user:App1I3cu54KCLdVlYKalVFZlIr4v9uvO@dpg-d6n3u8450q8c73atq74g-a.virginia-postgres.render.com/stockmarket_db_6z4q"

engine = create_engine(DATABASE_URL)

# ── Main ──────────────────────────────────────────────────────────────────────

def refresh():
    print("refresh_calculated_stocks: starting...")

    with engine.connect() as conn:
        rows = conn.execute(text("""
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
            JOIN sec        ON l.symbol = sec.symbol
            WHERE a.avg_volume_20d >= 500000
              AND l.price          >= 5
              AND mc.market_cap    >= 1000000000
        """)).fetchall()

    print(f"refresh_calculated_stocks: {len(rows)} symbols qualify for calculation")

    # Bulk upsert in batches of 100
    upsert_rows = []
    for r in rows:
        upsert_rows.append({
            "symbol":         r[0],
            "avg_volume_20d": int(r[1]),
            "volume_surge":   float(r[2]),
            "price_change":   float(r[3]),
            "sector":         str(r[6]) if r[6] else None,
        })

    updated = 0
    batch_size = 100

    for i in range(0, len(upsert_rows), batch_size):
        batch = upsert_rows[i:i + batch_size]
        with engine.connect() as conn:
            try:
                conn.execute(text("""
                    INSERT INTO calculated_stocks
                        (symbol, avg_volume_20d, volume_surge, price_change, sector, last_updated)
                    VALUES
                        (:symbol, :avg_volume_20d, :volume_surge, :price_change, :sector, NOW())
                    ON CONFLICT (symbol) DO UPDATE SET
                        avg_volume_20d = EXCLUDED.avg_volume_20d,
                        volume_surge   = EXCLUDED.volume_surge,
                        price_change   = EXCLUDED.price_change,
                        sector         = COALESCE(NULLIF(EXCLUDED.sector, ''), calculated_stocks.sector),
                        last_updated   = NOW()
                """), batch)
                conn.commit()
                updated += len(batch)
                print(f"  Batch {i // batch_size + 1}: upserted {len(batch)} rows (total: {updated})")
            except Exception as e:
                conn.rollback()
                print(f"  Batch upsert error: {e}")

    print(f"refresh_calculated_stocks: DONE — {updated} symbols updated")


if __name__ == "__main__":
    refresh()
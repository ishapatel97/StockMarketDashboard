import yfinance as yf
from app.services.universe_service import load_tickers
from concurrent.futures import ThreadPoolExecutor
import time
import json
import os
from datetime import datetime, timedelta
from backend.database import SessionLocal
from sqlalchemy import text
import pandas as pd

# Cache file path
CACHE_FILE = "stock_cache.json"
BATCH_INDEX_FILE = "batch_index.json"
CACHE_DURATION = 3600  # 1 hour in seconds
BATCH_SIZE = 200

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

def save_history(symbol, hist):

    db = SessionLocal()

    for date, row in hist.iterrows():

        db.execute(text("""
            INSERT INTO stock_prices(symbol, date, close_price, volume)
            VALUES(:symbol, :date, :price, :volume)
            ON CONFLICT(symbole, date) DO NOTHING
        """), {
            "symbol": symbol,
            "date": date,
            "price": row["Close"],
            "volume": row["Volume"]
        })
        print("Saving history for",symbol)

    db.commit()
    db.close()

def analyze_stock(ticker):
    time.sleep(0.2)
    try:
        #stock = yf.Ticker(ticker)
        #hist = stock.history(period="2mo")

        db = SessionLocal()

        # check if history already exists
        result = db.execute(text("""
            SELECT date, close_price, volume
            FROM stock_prices
            WHERE symbol = :symbol
            ORDER BY date DESC
            LIMIT 40
        """), {"symbol": ticker}).fetchall()

        db.close()

        if len(result) < 20:

            stock = yf.Ticker(ticker)
            hist = stock.history(period="2mo")

            if hist.empty:
                return None

            save_history(ticker, hist)

        else:

            hist = pd.DataFrame(result, columns=["Date", "Close", "Volume"])
            hist = hist.sort_values("Date")

        if hist.empty or len(hist) < 2:
            return None

        volumes = hist["Volume"].dropna()
        if len(volumes) < 5:
            return None
            
        avg_volume = volumes.rolling(20).mean().iloc[-1]
        today_volume = volumes.iloc[-1]

        if avg_volume == 0 or avg_volume != avg_volume:
            return None
        
        if avg_volume < 100000:
            return None

        if today_volume < 20000:
            return None

        volume_surge = ((today_volume - avg_volume) / avg_volume) * 100
        
        prices = hist["Close"].dropna()          
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
            "volume_surge": float(round(volume_surge, 2))
        }

    except Exception as e:
        return None

def get_top_stocks():
    # Don't use cache - process batches continuously
    print("Fetching fresh data...")
    all_tickers = load_tickers()
    
    # Get the next batch of tickers
    tickers, start_index, batch_num = get_next_batch(all_tickers)

    print(f"Loaded {len(tickers)} tickers (Batch {batch_num}, starting from index {start_index})")

    with ThreadPoolExecutor(max_workers=6) as executor:
        results = list(executor.map(analyze_stock, tickers))

    results = [r for r in results if r]

    print(f"Found {len(results)} stocks matching criteria")

    results.sort(key=lambda x: x["volume_surge"], reverse=True)

    top_stocks = results[:20]
    for stock_data in top_stocks:
        try:
            ticker = stock_data["symbol"]
            stock = yf.Ticker(ticker)

            info = stock.info
            market_cap = info.get("marketCap")

            if market_cap:
                stock_data["market_cap_billion"] = round(market_cap / 1_000_000_000, 2)
            else:
                stock_data["market_cap_billion"] = 0
        except:
            stock_data["market_cap_billion"] = 0

    return top_stocks
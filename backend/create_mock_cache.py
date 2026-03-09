import json

# Create mock stock data for testing
mock_stocks = [
    {
        "symbol": "AAPL",
        "price": 263.75,
        "price_change": -0.37,
        "market_cap_billion": 2500.0,
        "today_volume": 38386528,
        "avg_volume": 35000000,
        "volume_surge": 9.68
    },
    {
        "symbol": "MSFT",
        "price": 403.93,
        "price_change": 1.25,
        "market_cap_billion": 3000.0,
        "today_volume": 15000000,
        "avg_volume": 14000000,
        "volume_surge": 7.14
    },
    {
        "symbol": "GOOGL",
        "price": 303.58,
        "price_change": 0.85,
        "market_cap_billion": 1800.0,
        "today_volume": 18000000,
        "avg_volume": 16000000,
        "volume_surge": 12.5
    },
    {
        "symbol": "AMZN",
        "price": 208.73,
        "price_change": -1.2,
        "market_cap_billion": 2100.0,
        "today_volume": 42000000,
        "avg_volume": 38000000,
        "volume_surge": 10.53
    },
    {
        "symbol": "TSLA",
        "price": 392.43,
        "price_change": 2.5,
        "market_cap_billion": 1200.0,
        "today_volume": 120000000,
        "avg_volume": 100000000,
        "volume_surge": 20.0
    },
    {
        "symbol": "NVDA",
        "price": 875.20,
        "price_change": 3.2,
        "market_cap_billion": 2150.0,
        "today_volume": 35000000,
        "avg_volume": 30000000,
        "volume_surge": 16.67
    },
    {
        "symbol": "META",
        "price": 512.45,
        "price_change": 1.8,
        "market_cap_billion": 1300.0,
        "today_volume": 12000000,
        "avg_volume": 10000000,
        "volume_surge": 20.0
    },
    {
        "symbol": "NFLX",
        "price": 285.30,
        "price_change": -0.5,
        "market_cap_billion": 120.0,
        "today_volume": 2500000,
        "avg_volume": 2000000,
        "volume_surge": 25.0
    },
    {
        "symbol": "GOOG",
        "price": 303.50,
        "price_change": 0.9,
        "market_cap_billion": 1800.0,
        "today_volume": 17000000,
        "avg_volume": 15000000,
        "volume_surge": 13.33
    },
    {
        "symbol": "UBER",
        "price": 95.75,
        "price_change": 2.1,
        "market_cap_billion": 200.0,
        "today_volume": 25000000,
        "avg_volume": 20000000,
        "volume_surge": 25.0
    }
]

from datetime import datetime

# Save as cache
cache_data = {
    "timestamp": datetime.now().isoformat(),
    "stocks": mock_stocks
}

with open("stock_cache.json", "w") as f:
    json.dump(cache_data, f)

print("Mock cache created successfully!")
print(f"Saved {len(mock_stocks)} stocks")

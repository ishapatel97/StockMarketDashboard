import requests
import json
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

BASE_URL = "https://api.nasdaq.com/api/screener/stocks"


def fetch_all_exchange(exchange):
    tickers = []
    offset = 0
    limit = 1000

    while True:
        url = f"{BASE_URL}?exchange={exchange}&download=true&limit={limit}&offset={offset}"
        print("Fetching:", url)

        response = requests.get(url, headers=HEADERS)
        
        # Fix malformed JSON by adding missing commas between key-value pairs
        text = response.text
        # Add commas between closing and opening quotes when they're on the same line
        text = re.sub(r'"\s*\n\s*"', '",\n"', text)
        text = re.sub(r'"\s+"', '", "', text)
        
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # If still invalid, try the raw response
            data = response.json()

        # Parse the rows from the response
        rows = data.get("data", {}).get("rows", [])

        if not rows:
            break

        tickers.extend([row["symbol"] for row in rows])

        total = data.get("data", {}).get("totalrecords", 0)

        offset += limit

        if offset >= total:
            break

    return tickers


print("Downloading NASDAQ...")
nasdaq = fetch_all_exchange("NASDAQ")

print("Downloading NYSE...")
nyse = fetch_all_exchange("NYSE")

all_tickers = list(set(nasdaq + nyse))

with open("all_tickers.txt", "w") as f:
    for ticker in all_tickers:
        f.write(ticker + "\n")

print("Saved", len(all_tickers), "tickers to all_tickers.txt")
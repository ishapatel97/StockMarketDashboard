import requests
import json

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

url = "https://api.nasdaq.com/api/screener/stocks?exchange=NASDAQ&download=true&limit=10&offset=0"
print("Fetching:", url)

response = requests.get(url, headers=HEADERS)
print("Status Code:", response.status_code)
print("\nResponse JSON:")
print(json.dumps(response.json(), indent=2))

import requests
import json
from datetime import datetime

url = "https://gamma-api.polymarket.com/markets"
params = {"active": "true", "limit": 10, "q": "btc 5m"}

print("Querying Gamma...")
res = requests.get(url, params=params)
print(f"Status: {res.status_code}")

with open("gamma_results.json", "w") as f:
    json.dump(res.json(), f, indent=2)

print("Results saved to gamma_results.json")

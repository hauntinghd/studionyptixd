import requests
import json

def find_real_5m():
    # Try the main markets endpoint with filtering
    url = "https://gamma-api.polymarket.com/markets?active=true&limit=100"
    r = requests.get(url)
    data = r.json()
    
    for m in data:
        slug = m.get('slug', '')
        if '5m' in slug.lower():
            print(f"REAL 5M??: {slug}")
            print(f"End Date: {m.get('endDate')}")
            print(f"IDs: {m.get('clobTokenIds')}")

if __name__ == "__main__":
    find_real_5m()

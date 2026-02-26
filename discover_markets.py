import json
import time
from urllib.request import urlopen, Request

def get_5m_markets():
    print("Fetching active markets...")
    url = "https://gamma-api.polymarket.com/markets?active=true&limit=1000"
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urlopen(req, timeout=15) as res:
            data = json.loads(res.read().decode())
            print(f"Total active markets found: {len(data)}")
            m5 = [
                {
                    "slug": m.get('slug'),
                    "ids": m.get('clobTokenIds'),
                    "endDate": m.get('endDate')
                }
                for m in data 
                if '5m' in m.get('slug', '').lower() 
                and m.get('clobTokenIds')
            ]
            return m5
    except Exception as e:
        print(f"Error: {e}")
        return []

if __name__ == "__main__":
    markets = get_5m_markets()
    print(f"Found {len(markets)} 5m markets.")
    # Sort by end date to see most recent ones
    markets.sort(key=lambda x: x.get('endDate', ''))
    for m in markets[:30]:
        print(f"Slug: {m['slug']} | IDs: {m['ids']}")

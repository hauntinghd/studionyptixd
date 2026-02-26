import json
from urllib.request import urlopen, Request

def check():
    url = "https://gamma-api.polymarket.com/markets?active=true&limit=1000"
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    res = urlopen(req)
    data = json.loads(res.read().decode())
    m5 = [m.get('slug') for m in data if '5m' in m.get('slug', '').lower()]
    print(f"Found {len(m5)} 5m markets:")
    for slug in m5:
        print(slug)

if __name__ == "__main__":
    check()

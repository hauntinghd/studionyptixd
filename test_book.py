import urllib.request
import json
import time

ids = [
    '36046793853870709793511383708577769923735778034355418813949722699101291361306',
    '64936983663025340213743752621933496658163041836309906734014399629328918402577'
]

for tid in ids:
    url = f"https://clob.polymarket.com/book?token_id={tid}"
    print(f"Checking {tid[:10]}...")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as res:
            data = json.loads(res.read().decode())
            asks = data.get('asks', [])
            bids = data.get('bids', [])
            print(f"  Asks: {len(asks)} | Best Ask: {asks[0]['price'] if asks else 'N/A'}")
            print(f"  Bids: {len(bids)} | Best Bid: {bids[0]['price'] if bids else 'N/A'}")
    except Exception as e:
        print(f"  Error: {e}")

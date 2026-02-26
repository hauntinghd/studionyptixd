import requests
import time
import json

def get_market_info():
    now_ts = time.time()
    window_start = int(now_ts - (now_ts % 300))
    slug = f"btc-updown-5m-{window_start}"
    url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
    print(f"Fetching {url}...")
    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        if res.status_code != 200:
            print(f"Error: {res.status_code}")
            return
        data = res.json()
        markets = data.get('markets', [])
        for m in markets:
            print(f"Market: {m.get('question')}")
            print(f"Slug: {m.get('slug')}")
            ids = m.get('clobTokenIds')
            if isinstance(ids, str): ids = json.loads(ids)
            print(f"IDs: {ids}")
            
            for i, tid in enumerate(ids):
                book_url = f"https://clob.polymarket.com/book?token_id={tid}"
                print(f"  Checking Token {i}: {tid}")
                b_res = requests.get(book_url, headers={'User-Agent': 'Mozilla/5.0'})
                b_data = b_res.json()
                asks = b_data.get('asks', [])
                bids = b_data.get('bids', [])
                print(f"    Asks: {len(asks)} | Best Ask: {asks[0]['price'] if asks else 'N/A'}")
                print(f"    Bids: {len(bids)} | Best Bid: {bids[0]['price'] if bids else 'N/A'}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    get_market_info()

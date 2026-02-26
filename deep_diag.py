import requests
import json
import time

# Let's find the current active BTC 5m market first
url = "https://gamma-api.polymarket.com/search-v2?q=btc+5m&type=events&events_status=active"
try:
    res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    data = res.json()
    events = data.get('data', [])
    for ev in events:
        markets = ev.get('markets', [])
        for m in markets:
            slug = m.get('slug', '').lower()
            if 'btc' in slug and '5m' in slug:
                print(f"MARKET: {m.get('question')}")
                print(f"SLUG: {slug}")
                token_ids = m.get('clobTokenIds', [])
                print(f"IDS: {token_ids}")
                
                # Check each token
                for i, tid in enumerate(token_ids):
                    book_url = f"https://clob.polymarket.com/book?token_id={tid}"
                    book_res = requests.get(book_url, headers={'User-Agent': 'Mozilla/5.0'})
                    book_data = book_res.json()
                    
                    asks = book_data.get('asks', [])
                    bids = book_data.get('bids', [])
                    
                    print(f"  TOKEN {i} ({tid[:10]}...):")
                    print(f"    Asks Found: {len(asks)}")
                    if asks:
                        print(f"    Best Ask: {asks[0]['price']} (Size: {asks[0]['size']})")
                        # Print first 3 asks
                        for a in asks[:3]:
                            print(f"      {a['price']} x {a['size']}")
                    
                    print(f"    Bids Found: {len(bids)}")
                    if bids:
                        print(f"    Best Bid: {bids[0]['price']} (Size: {bids[0]['size']})")
                print("-" * 40)
except Exception as e:
    print(f"Error: {e}")

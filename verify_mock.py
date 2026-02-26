import requests
import json
import time

def check_real_price():
    # Slug from the screenshot: btc-updown-5m-1771893900
    # Let's try to find it via API first
    search_url = "https://gamma-api.polymarket.com/search-v2?q=5m&events_status=active"
    r = requests.get(search_url)
    data = r.json()
    
    found = False
    for event in data.get('events', []):
        for m in event.get('markets', []):
            if m.get('slug') == 'btc-updown-5m-1771893900':
                print(f"Found Market: {m['slug']}")
                print(f"Ended: {m.get('closed')}")
                print(f"End Date: {m.get('endDate')}")
                ids = m.get('clobTokenIds')
                print(f"IDs: {ids}")
                found = True
                break
        if found: break
    
    if not found:
        print("Market btc-updown-5m-1771893900 not found in active events.")
        # Try direct market lookup
        # Need the ID. Let's try to find ANY active 5m market.
        for event in data.get('events', []):
            for m in event.get('markets', []):
                if '5m' in m.get('slug', ''):
                    print(f"Testing active market: {m['slug']}")
                    ids = m.get('clobTokenIds')
                    if isinstance(ids, str): ids = json.loads(ids)
                    if ids:
                        u1 = f"https://clob.polymarket.com/price?token_id={ids[0]}&side=sell"
                        u2 = f"https://clob.polymarket.com/price?token_id={ids[1]}&side=sell"
                        p1 = requests.get(u1).json()
                        p2 = requests.get(u2).json()
                        print(f"P1: {p1}")
                        print(f"P2: {p2}")
                    return

if __name__ == "__main__":
    check_real_price()

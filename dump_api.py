import requests
import json
import time

def dump_search():
    url = "https://gamma-api.polymarket.com/search-v2?q=5m&events_status=active&limit_per_type=20"
    try:
        r = requests.get(url)
        data = r.json()
        print(f"Current System time.time(): {time.time()}")
        
        found_any = False
        for event in data.get('events', []):
            for m in event.get('markets', []):
                slug = m.get('slug', '')
                if '5m' in slug:
                    print(f"MARKET: {slug} | END: {m.get('endDate')} | STATUS: {m.get('active')}")
                    found_any = True
        
        if not found_any:
            print("No 5m markets found.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    dump_search()

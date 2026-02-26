import requests
import json

def check_one_market():
    mid = "460910" # Example from earlier or just try to find one
    # Let's find a 5m market ID from search first
    search_url = "https://gamma-api.polymarket.com/search-v2?q=5m"
    r = requests.get(search_url)
    data = r.json()
    
    first_id = None
    for e in data.get('events', []):
        for m in e.get('markets', []):
            if '5m' in m.get('slug', ''):
                first_id = m.get('id')
                print(f"Checking details for ID: {first_id} (Slug: {m.get('slug')})")
                break
        if first_id: break
    
    if first_id:
        detail_url = f"https://gamma-api.polymarket.com/markets/{first_id}"
        det = requests.get(detail_url).json()
        print(json.dumps(det, indent=2))

if __name__ == "__main__":
    check_one_market()

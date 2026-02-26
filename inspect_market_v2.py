import requests
import json

def inspect_market():
    url = "https://gamma-api.polymarket.com/search-v2?q=5m&events_status=active&limit_per_type=20"
    try:
        r = requests.get(url)
        data = r.json()
        if isinstance(data, dict) and 'events' in data:
            for e in data['events']:
                for m in e.get('markets', []):
                    if '5m' in m.get('slug', ''):
                        print(f"Market Name: {m.get('slug')}")
                        print(f"Market Keys: {m.keys()}")
                        # Print some sample values
                        for k in m.keys():
                            if 'id' in k.lower() or 'token' in k.lower():
                                print(f"  {k}: {m.get(k)}")
                        # Also check if it's under 'clobTokenIds' or similar
                        return
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    inspect_market()

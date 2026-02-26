import requests
import json

def dump_search():
    url = "https://gamma-api.polymarket.com/search-v2?q=5m&events_status=active&limit_per_type=20"
    try:
        r = requests.get(url)
        data = r.json()
        print(f"Type: {type(data)}")
        if isinstance(data, dict):
            print(f"Keys: {data.keys()}")
            # Check for 'events' key
            if 'events' in data:
                print(f"Events found: {len(data['events'])}")
                for e in data['events'][:2]:
                    print(f"Event: {e.get('title')} | Slugs: {[m.get('slug') for m in e.get('markets', [])]}")
        elif isinstance(data, list):
            print(f"List length: {len(data)}")
            if len(data) > 0:
                print(f"First element type: {type(data[0])}")
                print(f"First element keys: {data[0].keys()}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    dump_search()

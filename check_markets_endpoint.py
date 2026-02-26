import requests
import json

def check_markets_endpoint():
    print("Checking /markets endpoint...")
    url = "https://gamma-api.polymarket.com/markets?active=true&limit=10&search=5m"
    try:
        r = requests.get(url)
        data = r.json()
        print(f"Type: {type(data)}")
        if isinstance(data, list) and len(data) > 0:
            m = data[0]
            print(f"Keys: {m.keys()}")
            print(f"clobTokenIds: {m.get('clobTokenIds')}")
            print(f"Slug: {m.get('slug')}")
        else:
            print(f"No results or empty: {data}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    check_markets_endpoint()

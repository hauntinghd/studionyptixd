import requests
import json

def test_discovery():
    queries = ["5m", "bitcoin", "ethereum", "price", "updown"]
    for q in queries:
        url = f"https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=50&search={q}"
        print(f"Searching: {url}")
        try:
            r = requests.get(url)
            data = r.json()
            print(f"Found {len(data)} markets for {q}")
            for m in data[:3]:
                print(f"  - {m.get('slug')} | {m.get('question')}")
        except Exception as e:
            print(f"Error for {q}: {e}")

if __name__ == '__main__':
    test_discovery()

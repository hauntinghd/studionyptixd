import requests
import json

def check_all_active():
    url = "https://gamma-api.polymarket.com/markets?active=true&limit=100"
    r = requests.get(url)
    data = r.json()
    count = 0
    for m in data:
        slug = m.get('slug', '').lower()
        if '5m' in slug or '5-minute' in slug:
            print(f"Found 5m: {slug}")
            count += 1
    print(f"Total 5m: {count}")

if __name__ == '__main__':
    check_all_active()

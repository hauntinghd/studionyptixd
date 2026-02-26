from urllib.request import urlopen, Request
import json
import time

def dump_gamma():
    print("Dumping Gamma BTC 5m results...")
    url = "https://gamma-api.polymarket.com/markets?active=true&limit=100&q=btc"
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    
    try:
        with urlopen(req) as response:
            data = json.loads(response.read().decode())
            with open("gamma_dump.json", "w") as f:
                json.dump(data, f, indent=2)
            print(f"Dumped {len(data)} markets to gamma_dump.json")
            
            # Print slugs of btc markets
            for m in data:
                slug = m.get('slug', '')
                if '5m' in slug or '5-min' in slug or 'minute' in m.get('question', ''):
                    print(f"MATCH: {slug} | End: {m.get('endDate')} | Active: {m.get('active')}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    dump_gamma()

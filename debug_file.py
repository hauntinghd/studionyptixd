import requests
import json
import time

def run_debug():
    output = []
    now_ts = time.time()
    window_start = int(now_ts - (now_ts % 300))
    slug = f"btc-updown-5m-{window_start}"
    url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
    output.append(f"URL: {url}")
    
    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        output.append(f"Status: {res.status_code}")
        if res.status_code == 200:
            data = res.json()
            markets = data.get('markets', [])
            for m in markets:
                output.append(f"Market: {m.get('question')}")
                ids = m.get('clobTokenIds')
                if isinstance(ids, str): ids = json.loads(ids)
                output.append(f"IDs: {ids}")
                
                for tid in ids:
                    b_url = f"https://clob.polymarket.com/book?token_id={tid}"
                    output.append(f"  Checking {tid}")
                    b_res = requests.get(b_url, headers={'User-Agent': 'Mozilla/5.0'})
                    b_data = b_res.json()
                    asks = b_data.get('asks', [])
                    output.append(f"    Best Ask: {asks[0]['price'] if asks else 'N/A'}")
    except Exception as e:
        output.append(f"Error: {e}")
    
    with open('debug_output.txt', 'w') as f:
        f.write('\n'.join(output))

if __name__ == "__main__":
    run_debug()

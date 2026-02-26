from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
import sys

client = ClobClient("https://clob.polymarket.com", chain_id=POLYGON)
cursor = "MA=="

print("Starting deep scan (100 pages)...", flush=True)

for i in range(100):
    try:
        res = client.get_markets(next_cursor=cursor)
        if not isinstance(res, dict):
            print(f"Page {i}: Unexpected response type {type(res)}", flush=True)
            break
            
        markets = res.get('data', [])
        cursor = res.get('next_cursor', '')
        
        btc_count = sum(1 for m in markets if 'btc' in m.get('market_slug', '').lower())
        if btc_count > 0:
             print(f"Page {i}: Found {btc_count} BTC markets. Cursor: {cursor}", flush=True)
             # Print one example
             for m in markets:
                 if 'btc' in m.get('market_slug', '').lower():
                     print(f"  - {m.get('market_slug')}", flush=True)
                     break
        
        if not cursor or cursor == "MA==":
            print("Reached end of markets.", flush=True)
            break
            
    except Exception as e:
        print(f"Error on page {i}: {e}", flush=True)
        break

print("Scan complete.", flush=True)

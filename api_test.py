from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
import time

print("Vibe Check: Testing API Speed...")
client = ClobClient("https://clob.polymarket.com", chain_id=POLYGON)

start = time.time()
try:
    res = client.get_markets()
    end = time.time()
    print(f"API Response in {end-start:.2f} seconds.")
    if isinstance(res, dict):
        markets = res.get('data', [])
        print(f"Found {len(markets)} markets on page 1.")
        for m in markets[:5]:
            print(f" - {m.get('market_slug')}")
    else:
        print(f"Unexpected response: {type(res)}")
except Exception as e:
    print(f"API Error: {e}")

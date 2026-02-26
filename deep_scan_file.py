from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
import json

client = ClobClient("https://clob.polymarket.com", chain_id=POLYGON)

with open("results.txt", "w") as f:
    f.write("Starting deep scan...\n")
    current_cursor = ""
    for page in range(10):
        f.write(f"Page {page+1}...\n")
        try:
            res = client.get_markets(next_cursor=current_cursor)
            markets = res.get('data', [])
            current_cursor = res.get('next_cursor', '')
            for m in markets:
                slug = m.get('market_slug', '')
                if 'btc' in slug.lower():
                    f.write(f"FOUND: {slug}\n")
            if not current_cursor or current_cursor == "MA==":
                break
        except Exception as e:
            f.write(f"ERROR: {e}\n")
f.write("Scan complete.\n")

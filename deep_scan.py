from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON

client = ClobClient("https://clob.polymarket.com", chain_id=POLYGON)

current_cursor = ""
found = False

print("Vibe Check: Deep Scanning Polymarket for BTC Slugs...")

for page in range(5):
    print(f"Scanning Page {page+1}...")
    res = client.get_markets(next_cursor=current_cursor)
    markets = res.get('data', [])
    current_cursor = res.get('next_cursor', '')
    
    for m in markets:
        slug = m.get('market_slug', '')
        if 'btc' in slug.lower():
            print(f" [FOUND] {slug} | {m.get('question')}")
            found = True
            
    if not current_cursor or current_cursor == "MA==":
        break

if not found:
    print("No BTC markets found in first 5 pages. This is unexpected.")

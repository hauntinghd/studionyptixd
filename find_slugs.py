from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON

client = ClobClient("https://clob.polymarket.com", chain_id=POLYGON)
res = client.get_markets()
markets = res.get('data', [])

print(f"Total markets on page 1: {len(markets)}")
print("Searching for BTC related slugs...")
for m in markets:
    slug = m.get('market_slug', '')
    if 'btc' in slug.lower():
        print(f" - {slug} | Question: {m.get('question')}")

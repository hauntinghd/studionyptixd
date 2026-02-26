from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
import json

client = ClobClient("https://clob.polymarket.com", chain_id=POLYGON)
res = client.get_markets()
print(f"Response type: {type(res)}")
if isinstance(res, dict):
    print(f"Keys: {list(res.keys())}")
    if 'data' in res:
        print(f"Number of markets: {len(res['data'])}")
    print(f"Next cursor: {res.get('next_cursor')}")
elif isinstance(res, str):
    print("Response is a string. First 100 chars:")
    print(res[:100])

from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
import json

client = ClobClient("https://clob.polymarket.com", chain_id=POLYGON)
cid = "0xaf8c42b9a7a0c822db0f415febb15d936ea1515fb5bf56fae9d368ea14ab8378"

print(f"Fetching market by condition_id: {cid}")
try:
    res = client.get_market(cid)
    print("Success!")
    print(json.dumps(res, indent=2))
except Exception as e:
    print(f"Error: {e}")

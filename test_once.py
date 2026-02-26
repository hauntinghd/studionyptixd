from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
import json

print("Initializing client...")
host = "https://clob.polymarket.com"
client = ClobClient(host, chain_id=POLYGON)

print("Calling get_markets()...")
try:
    # Just get one market or use a timeout if possible
    res = client.get_markets()
    print("Call successful!")
    print(f"Data type: {type(res)}")
except Exception as e:
    print(f"Error: {e}")

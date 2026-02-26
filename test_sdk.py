from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
import json

host = "https://clob.polymarket.com"
client = ClobClient(host, chain_id=POLYGON)

try:
    print("Testing get_markets()...")
    res = client.get_markets()
    print(f"Result type: {type(res)}")
    # If it's a string, it might be JSON string?
    if isinstance(res, str):
        print("Result is a string, attempting JSON parse...")
        try:
            res = json.loads(res)
            print("Successfully parsed JSON string.")
        except:
            print("Failed to parse JSON string.")
    
    print(f"Final Data Type: {type(res)}")
    if isinstance(res, list):
        print(f"First element: {res[0]}")
    elif isinstance(res, dict):
        print(f"Keys: {res.keys()}")
except Exception as e:
    print(f"Error: {e}")

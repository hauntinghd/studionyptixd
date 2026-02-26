from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
import json

host = "https://clob.polymarket.com"
client = ClobClient(host, chain_id=POLYGON)

try:
    markets = client.get_markets()
    with open("market_debug.txt", "w") as f:
        f.write(f"Type: {type(markets)}\n")
        if markets:
            f.write(f"First element type: {type(markets[0])}\n")
            f.write(f"First element: {markets[0]}\n")
            if isinstance(markets[0], dict):
                f.write(f"Keys: {markets[0].keys()}\n")
except Exception as e:
    with open("market_debug.txt", "w") as f:
        f.write(f"Error: {e}\n")

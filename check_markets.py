from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON

host = "https://clob.polymarket.com"
client = ClobClient(host, chain_id=POLYGON)

try:
    markets = client.get_markets()
    print(f"Type of markets: {type(markets)}")
    if len(markets) > 0:
        print(f"Type of first element: {type(markets[0])}")
        print(f"First element: {markets[0]}")
except Exception as e:
    print(f"Error: {e}")

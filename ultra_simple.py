from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
import json
import time
import asyncio

async def test():
    print("Vibe Check: Starting ultra-simple watcher...")
    host = "https://clob.polymarket.com"
    client = ClobClient(host, chain_id=POLYGON)
    
    while True:
        try:
            print("Fetching markets...")
            res = client.get_markets()
            if isinstance(res, dict):
                markets = res.get('data', [])
                print(f"Found {len(markets)} markets.")
                
                btc_markets = [m for m in markets if "btc-5-minute-up-or-down" in m.get('market_slug', '')]
                print(f"Found {len(btc_markets)} matching BTC markets.")
                
                if btc_markets:
                    m = btc_markets[0]
                    print(f"Sample BTC market: {m.get('question')}")
                    tokens = m.get('tokens', [])
                    print(f"Tokens: {tokens}")
            else:
                print(f"Response is not a dict: {type(res)}")
        except Exception as e:
            print(f"ERROR: {e}")
        
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(test())

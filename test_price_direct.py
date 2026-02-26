from urllib.request import urlopen, Request
import json
import asyncio

async def test_price():
    token_id = "16765904416762900167722673073854320233197974321017559931165300133829356260026"
    url = f"https://clob.polymarket.com/price?token_id={token_id}&side=sell"
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    print(f"Fetching {url}")
    try:
        with urlopen(req, timeout=5) as res:
            data = json.loads(res.read().decode())
            print(f"Data: {data}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_price())

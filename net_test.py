import urllib.request
print("Starting network test...")
try:
    with urllib.request.urlopen("https://gamma-api.polymarket.com/events/slug/btc-updown-5m-1771816200", timeout=5) as res:
        print(f"Status: {res.status}")
        print(f"Data snippet: {res.read(100)}")
except Exception as e:
    print(f"Error: {e}")

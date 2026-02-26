import os
import sys

with open("boot_check.txt", "w") as f:
    f.write("Booting diagnostic...\n")
    try:
        import py_clob_client
        f.write("py_clob_client imported\n")
    except Exception as e:
        f.write(f"py_clob_client failed: {e}\n")
    
    try:
        from urllib.request import urlopen, Request
        import json
        f.write("urllib and json imported\n")
        url = "https://gamma-api.polymarket.com/markets?active=true&q=btc+5m&limit=1"
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urlopen(req) as response:
            data = json.loads(response.read().decode())
            f.write(f"Discovery successful: {len(data)} markets found\n")
    except Exception as e:
        f.write(f"Discovery failed: {e}\n")

    f.write("Diagnostic complete.\n")

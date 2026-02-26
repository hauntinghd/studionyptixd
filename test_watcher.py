import asyncio
from shadow_watcher import ShadowWatcher

async def test():
    print("Initializing Watcher...")
    watcher = ShadowWatcher()
    print("Finding latest market...")
    ids = await watcher.find_latest_market()
    print(f"IDs: {ids}")
    if ids and len(ids) >= 2:
        print("Fetching prices...")
        up = await watcher.get_price(ids[0])
        down = await watcher.get_price(ids[1])
        print(f"UP: {up} | DOWN: {down}")

if __name__ == "__main__":
    asyncio.run(test())

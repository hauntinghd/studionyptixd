import asyncio
from shadow_watcher import ShadowWatcher
import logging

async def main():
    print("Initializing ShadowWatcher...")
    watcher = ShadowWatcher()
    print("Testing find_latest_market...")
    assets = await watcher.find_latest_market()
    print(f"Results: {assets}")
    if assets:
        print("Success! Found markers.")
    else:
        print("Nothing found in this test run.")

if __name__ == "__main__":
    asyncio.run(main())

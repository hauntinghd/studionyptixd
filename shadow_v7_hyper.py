import os
import time
import json
import asyncio
import sqlite3
import logging
from urllib.request import urlopen, Request
from datetime import datetime
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler("shadow_hyper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ShadowHyperWatcher:
    def __init__(self):
        self.markets: Dict[str, Dict[str, Any]] = {}  # slug -> info
        self.db_path = "shadow_data.db"
        self._init_db()
        self.reversal_threshold = 0.05
        self.last_discovery_ts = 0.0
        self.discovery_interval = 300.0 # 5 minutes
        self.executor = ThreadPoolExecutor(max_workers=50)

    def _init_db(self):
        """Initialize SQLite database for training and tracking"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS virtual_trades (
                        slug TEXT,
                        type TEXT, -- 'ARB' or 'REVERSAL'
                        token_id TEXT,
                        price REAL,
                        timestamp INTEGER,
                        status TEXT,
                        pnl REAL,
                        PRIMARY KEY (slug, type, timestamp)
                    )
                """)
                conn.commit()
        except Exception as e:
            logger.error(f"DB Init Error: {e}")

    def _sync_fetch(self, url: str) -> Any:
        try:
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urlopen(req, timeout=10) as res:
                return json.loads(res.read().decode())
        except:
            return None

    async def fetch_json(self, url: str) -> Any:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._sync_fetch, url)

    async def discover_markets(self):
        """Find all active 5-minute crypto markets"""
        if time.time() - self.last_discovery_ts < self.discovery_interval:
            return

        logger.info("Scanning for new 5m markets...")
        try:
            url = "https://gamma-api.polymarket.com/markets?active=true&limit=1000"
            data = await self.fetch_json(url)
            
            if not data: return

            new_markets = {}
            for m in data:
                slug = m.get('slug', '').lower()
                # Track ALL 5m markets regardless of asset class
                if '5m' in slug:
                    ids = m.get('clobTokenIds')
                    if isinstance(ids, str):
                        try: ids = json.loads(ids)
                        except: continue
                    
                    if ids and len(ids) >= 2:
                        new_markets[slug] = {
                            "title": m.get('question', m.get('title', slug)),
                            "ids": ids,
                            "endDate": m.get('endDate')
                        }
            
            self.markets = new_markets
            self.last_discovery_ts = time.time()
            logger.info(f"Tracking {len(self.markets)} active 5m markets.")
        except Exception as e:
            logger.error(f"Discovery Error: {e}")

    def log_trade(self, slug: str, trade_type: str, token_id: str, price: float):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO virtual_trades 
                    (slug, type, token_id, price, timestamp, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (slug, trade_type, token_id, price, int(time.time()), 'OPEN'))
                conn.commit()
        except Exception as e:
            logger.error(f"DB Log Error: {e}")

    async def watch_cycle(self):
        # 1. Discovery
        await self.discover_markets()

        if not self.markets:
            logger.warning("No markets found to watch. Retrying discovery...")
            self.last_discovery_ts = 0.0
            return

        # 2. Gather Prices Parallel
        tasks = []
        slug_order = []
        for slug, info in self.markets.items():
            slug_order.append(slug)
            tasks.append(self.fetch_json(f"https://clob.polymarket.com/price?token_id={info['ids'][0]}&side=sell"))
            tasks.append(self.fetch_json(f"https://clob.polymarket.com/price?token_id={info['ids'][1]}&side=sell"))

        results = await asyncio.gather(*tasks)

        # 3. Analyze
        for i, slug in enumerate(slug_order):
            res_up = results[i*2]
            res_down = results[i*2 + 1]
            
            up_p = float(res_up.get('price', 0)) if res_up and res_up.get('price') else 0.0
            down_p = float(res_down.get('price', 0)) if res_down and res_down.get('price') else 0.0
            
            if up_p <= 0 or down_p <= 0: continue

            # Arb Check
            if (up_p + down_p) < 0.998:
                logger.info(f"!!! ARB !!! {slug}: {up_p} + {down_p} = {up_p+down_p}")
                self.log_trade(slug, 'ARB', 'BOTH', up_p + down_p)

            # Reversal Check (Tweet Strategy)
            if up_p < self.reversal_threshold:
                logger.info(f"!!! REVERSAL UP !!! {slug} at ${up_p}")
                self.log_trade(slug, 'REVERSAL', self.markets[slug]['ids'][0], up_p)
            
            if down_p < self.reversal_threshold:
                logger.info(f"!!! REVERSAL DOWN !!! {slug} at ${down_p}")
                self.log_trade(slug, 'REVERSAL', self.markets[slug]['ids'][1], down_p)

    async def run(self):
        logger.info("Starting Shadow Hyper-Watcher V7...")
        while True:
            t0 = time.time()
            try:
                await self.watch_cycle()
            except Exception as e:
                logger.error(f"Cycle Error: {e}")
            
            dt = time.time() - t0
            wait = max(0.5, 1.0 - dt)
            await asyncio.sleep(wait)

if __name__ == "__main__":
    watcher = ShadowHyperWatcher()
    asyncio.run(watcher.run())

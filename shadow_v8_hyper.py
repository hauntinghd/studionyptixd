import os
import time
import json
import asyncio
import sqlite3
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler("shadow_hyper_v8.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PostgresManager:
    def __init__(self, dsn="dbname=postgres user=postgres password=postgres host=localhost"):
        self.dsn = dsn
        self.enabled = False
        self._init_db()

    def _init_db(self):
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    # Training table: raw price history for building models
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS price_training_data (
                            id SERIAL PRIMARY KEY,
                            timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            slug TEXT,
                            token_id TEXT,
                            price REAL,
                            total_sum REAL,
                            liquidity REAL,
                            volume REAL
                        );
                    """)
                    # Actual signals/trades
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS model_signals (
                            id SERIAL PRIMARY KEY,
                            timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            slug TEXT,
                            type TEXT,
                            token_id TEXT,
                            price REAL,
                            status TEXT
                        );
                    """)
                conn.commit()
            self.enabled = True
            logger.info("PostgreSQL Database Online and Ready for Training Data.")
        except Exception as e:
            logger.warning(f"PostgreSQL Offline (Falling back to SQLite only): {e}")

    def log_price_data(self, slug, token_id, price, total_sum, liquidity, volume):
        if not self.enabled: return
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO price_training_data (slug, token_id, price, total_sum, liquidity, volume)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (slug, token_id, price, total_sum, liquidity, volume))
                conn.commit()
        except: pass

    def log_signal(self, slug, sig_type, token_id, price):
        if not self.enabled: return
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO model_signals (slug, type, token_id, price, status)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (slug, sig_type, token_id, price, 'OPEN'))
                conn.commit()
        except: pass

class ShadowHyperWatcher:
    def __init__(self):
        self.markets: Dict[str, Dict[str, Any]] = {}  # slug -> info
        self.db_path = "shadow_data.db"
        self._init_sqlite()
        self.pg = PostgresManager()
        self.reversal_threshold = 0.08 
        self.arb_threshold = 0.998
        self.last_discovery_ts = 0.0
        self.discovery_interval = 60.0 
        self.executor = ThreadPoolExecutor(max_workers=50)

    def _init_sqlite(self):
        """Initialize local SQLite database for quick tracking"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS virtual_trades (
                        slug TEXT,
                        type TEXT,
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
            logger.error(f"SQLite Init Error: {e}")

    def _sync_fetch(self, url: str) -> Any:
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            res = requests.get(url, headers=headers, timeout=5)
            if res.status_code == 200:
                return res.json()
            return None
        except Exception as e:
            return None

    async def fetch_json(self, url: str) -> Any:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._sync_fetch, url)

    async def discover_markets(self):
        """Find active, high-liquidity 5-minute markets"""
        if time.time() - self.last_discovery_ts < self.discovery_interval:
            return

        now_ts = time.time()
        logger.info(f"Scanning for Active 5m Markets (System Time: {int(now_ts)})...")
        
        # 1. Get potential 5m events
        search_url = "https://gamma-api.polymarket.com/search-v2?q=5m&events_status=active&limit_per_type=30"
        search_data = await self.fetch_json(search_url)
        
        market_ids = []
        if search_data and isinstance(search_data, dict) and 'events' in search_data:
            for item in search_data['events']:
                for m in item.get('markets', []):
                    slug = m.get('slug', '').lower()
                    if '5m' in slug:
                        mid = m.get('id')
                        if mid: market_ids.append(mid)

        logger.info(f"Checking details for {len(market_ids)} candidates...")
        
        # 2. Fetch full details to filter for legitimacy
        detail_tasks = [self.fetch_json(f"https://gamma-api.polymarket.com/markets/{mid}") for mid in market_ids]
        details = await asyncio.gather(*detail_tasks)
        
        new_markets = {}
        for m in details:
            if not m or not isinstance(m, dict): continue
            
            # CRITICAL: Filter for actually active and valid markets
            if not m.get('active') or m.get('closed'): continue
            
            # Check End Date
            end_str = m.get('endDate')
            if end_str:
                try:
                    # Format: 2026-02-24T16:55:00Z
                    end_dt = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                    if end_dt.timestamp() < now_ts:
                        continue # Past market
                except: pass

            # Liquidity Check (Legitimate pricing discovery)
            liq = float(m.get('liquidityNum', 0))
            if liq < 200: # Increase this for higher quality
                continue
                
            slug = m.get('slug', '').lower()
            ids = m.get('clobTokenIds')
            if isinstance(ids, str):
                try: ids = json.loads(ids)
                except: continue
            
            if ids and len(ids) >= 2:
                new_markets[slug] = {
                    "title": m.get('question', m.get('title', slug)),
                    "ids": ids,
                    "endDate": end_str,
                    "liquidity": liq,
                    "volume": float(m.get('volumeNum', 0))
                }

        self.markets = new_markets
        self.last_discovery_ts = time.time()
        if self.markets:
            logger.info(f"Tracking {len(self.markets)} legitimate active 5m markets.")
        else:
            logger.warning("No legitimate 5m markets found. Ensuring search fallback...")
            self.last_discovery_ts = 0.0

    def log_trade(self, slug: str, trade_type: str, token_id: str, price: float):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO virtual_trades 
                    (slug, type, token_id, price, timestamp, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (slug, trade_type, token_id, price, int(time.time()), 'OPEN'))
                conn.commit()
        except: pass

    async def watch_cycle(self):
        await self.discover_markets()
        if not self.markets: return

        # Sort by proximity to expiration (closest first)
        def get_end_ts(slug):
            try: return datetime.fromisoformat(self.markets[slug]['endDate'].replace('Z', '+00:00')).timestamp()
            except: return 1e15
            
        slug_order = sorted(self.markets.keys(), key=get_end_ts)
        
        # Gather Prices Parallel
        tasks = []
        for slug in slug_order:
            tasks.append(self.fetch_json(f"https://clob.polymarket.com/price?token_id={self.markets[slug]['ids'][0]}&side=sell"))
            tasks.append(self.fetch_json(f"https://clob.polymarket.com/price?token_id={self.markets[slug]['ids'][1]}&side=sell"))

        results = await asyncio.gather(*tasks)

        # Analyze
        for i, slug in enumerate(slug_order):
            res_up = results[i*2]
            res_down = results[i*2 + 1]
            info = self.markets.get(slug, {})
            
            up_p = float(res_up.get('price', 0)) if res_up and res_up.get('price') else 0.0
            down_p = float(res_down.get('price', 0)) if res_down and res_down.get('price') else 0.0
            
            if up_p <= 0 or down_p <= 0: continue
            
            total_sum = up_p + down_p

            # 1. PostgreSQL Training Data Log (Raw prices)
            if 'ids' in info:
                self.pg.log_price_data(slug, info['ids'][0], up_p, total_sum, info.get('liquidity', 0), info.get('volume', 0))
                self.pg.log_price_data(slug, info['ids'][1], down_p, total_sum, info.get('liquidity', 0), info.get('volume', 0))

            # 2. Performance Tracking / Signal detection
            if i < 2: # Reduce log spam, show top 2
                is_stale = " (STALE?)" if (abs(up_p - 0.51) < 0.001 and abs(down_p - 0.51) < 0.001) else ""
                logger.info(f"Live Price [{slug}]: {up_p:.4f} + {down_p:.4f} = {total_sum:.4f}{is_stale}")
            
            # Signal Logic
            if total_sum < self.arb_threshold and up_p > 0.01 and down_p > 0.01:
                logger.info(f"ALGO: Arb Opportunity Found! {slug} Sum: {total_sum:.4f}")
                self.log_trade(slug, 'ARB', 'BOTH', total_sum)
                self.pg.log_signal(slug, 'ARB', 'BOTH', total_sum)

            if 'ids' in info:
                # Reversal Hunter
                if up_p < self.reversal_threshold:
                    logger.info(f"ALGO: Low-Price Signal (UP) in {slug} at ${up_p:.3f}")
                    self.log_trade(slug, 'REVERSAL', info['ids'][0], up_p)
                    self.pg.log_signal(slug, 'REVERSAL', info['ids'][0], up_p)
                
                if down_p < self.reversal_threshold:
                    logger.info(f"ALGO: Low-Price Signal (DOWN) in {slug} at ${down_p:.3f}")
                    self.log_trade(slug, 'REVERSAL', info['ids'][1], down_p)
                    self.pg.log_signal(slug, 'REVERSAL', info['ids'][1], down_p)

    async def run(self):
        logger.info("Shadow Hyper-Watcher V8: Global 5m Discovery Online")
        while True:
            t0 = time.time()
            try:
                await self.watch_cycle()
            except Exception as e:
                logger.error(f"Cycle Error: {e}")
            
            dt = time.time() - t0
            wait = max(0.05, 0.5 - dt)
            await asyncio.sleep(wait)

if __name__ == "__main__":
    watcher = ShadowHyperWatcher()
    asyncio.run(watcher.run())

import os
import time
import json
import asyncio
import logging
import sqlite3
from urllib.request import urlopen, Request
from datetime import datetime
from typing import List

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler("shadow_sniper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ShadowWatcher:
    def __init__(self):
        self.active_assets = []
        self.current_market_name = "Wait for Sync..."
        self.current_slug = "unknown"
        self.db_path = "shadow_data.db"
        self._init_db()

    def _init_db(self):
        """Initialize SQLite database for training and tracking"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS virtual_arbs (
                    slug TEXT PRIMARY KEY,
                    start_ts INTEGER,
                    up_price REAL,
                    down_price REAL,
                    total_cost REAL,
                    profit_pct REAL,
                    status TEXT,
                    winner TEXT,
                    pnl REAL
                )
            """)
            conn.commit()

    async def find_latest_market(self):
        """Find the currently active 5-minute BTC market"""
        try:
            now_ts = time.time()
            window_start = int(now_ts - (now_ts % 300))
            
            # Predict the current and next slugs
            predicted_slugs = [
                f"btc-updown-5m-{window_start}",
                f"btc-updown-5m-{window_start + 300}",
                f"eth-updown-5m-{window_start}",
                f"eth-updown-5m-{window_start + 300}"
            ]
            
            candidates = []
            # 1. Search by predicted slugs (Fastest)
            for slug in predicted_slugs:
                try:
                    url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
                    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                    with urlopen(req, timeout=5) as res:
                        data = json.loads(res.read().decode())
                        m_list = data.get('markets', [])
                        if m_list:
                            candidates.extend(m_list)
                except: continue

            # 2. General search for '5m' markets (More comprehensive)
            try:
                url = "https://gamma-api.polymarket.com/markets?active=true&limit=100"
                req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urlopen(req, timeout=5) as res:
                    data = json.loads(res.read().decode())
                    if isinstance(data, list):
                        for m in data:
                            m_slug = m.get('slug', '').lower()
                            if '5m' in m_slug:
                                candidates.append(m)
            except: pass

            best_market = None
            soonest_end = float('inf')
            
            for m in candidates:
                if not isinstance(m, dict): continue
                slug = m.get('slug', '').lower()
                if ('btc' in slug or 'eth' in slug) and '5m' in slug:
                    try:
                        end_str = m.get('endDate', m.get('end_date_iso', ''))
                        if not end_str: continue
                        if end_str.endswith('Z'): end_str = end_str[:-1] + '+00:00'
                        end_ts = datetime.fromisoformat(end_str).timestamp()
                        
                        if end_ts > (now_ts + 2):
                            if end_ts < soonest_end:
                                best_market = m
                                soonest_end = end_ts
                    except Exception: continue

            if best_market:
                raw_ids = best_market.get('clobTokenIds', [])
                if isinstance(raw_ids, str):
                    try: asset_ids = json.loads(raw_ids)
                    except: asset_ids = []
                else: asset_ids = raw_ids
                
                m_name = str(best_market.get('question', best_market.get('title', 'Unknown')))
                m_slug = str(best_market.get('slug', 'unknown'))
                
                if asset_ids and len(asset_ids) >= 2:
                    if asset_ids != self.active_assets:
                        print(f"[LOCKED] {m_name}")
                        self.active_assets = list(asset_ids)
                        self.current_market_name = m_name
                        self.current_slug = m_slug
            
            return list(self.active_assets) if self.active_assets else []
        except Exception as e:
            print(f"[SNIPER ERROR] discovery: {e}")
            return list(self.active_assets)

    def log_opportunity(self, up_p: float, down_p: float, total_c: float):
        """Record the arb opportunity to SQLite"""
        try:
            profit = 1.0 - total_c
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO virtual_arbs 
                    (slug, start_ts, up_price, down_price, total_cost, profit_pct, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (self.current_slug, int(time.time()), up_p, down_p, total_c, profit, 'OPEN'))
                conn.commit()
        except Exception as e:
            print(f"Log Error: {e}")

    async def check_resolutions(self):
        """Check if old virtual trades have settled"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                rows = conn.execute("SELECT slug, total_cost FROM virtual_arbs WHERE status='OPEN'").fetchall()
                for slug, cost in rows:
                    try:
                        url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
                        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                        with urlopen(req, timeout=5) as res:
                            data = json.loads(res.read().decode())
                            if data.get('closed', False):
                                pnl = 1.0 - cost
                                conn.execute("UPDATE virtual_arbs SET status='CLOSED', pnl=? WHERE slug=?", (pnl, slug))
                                print(f"[TRAINING] Settled {slug} | Virtual PnL: ${pnl:.4f}")
                    except: continue
                conn.commit()
        except Exception as e:
            print(f"Resolution Error: {e}")

    def calculate_arb(self, up_p: float, down_p: float):
        """Log Arbs if found"""
        total_cost = up_p + down_p + 0.002
        if total_cost < 1.00:
            print(f"!!! ARB FOUND: ${total_cost:.4f} !!!")
            self.log_opportunity(up_p, down_p, total_cost)
            return True
        return False

    def display_dashboard(self):
        """Print current training database stats"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                stats = conn.execute("SELECT COUNT(*), SUM(pnl) FROM virtual_arbs").fetchone()
                total, pnl = stats
                pnl = pnl if pnl else 0.0
                print("\n" + "="*40)
                print(f" LIVE TRAINING DASHBOARD")
                print(f" Total Gaps Logged: {total}")
                print(f" Total Virtual PnL: ${pnl:.4f}")
                print("="*40 + "\n")
        except: pass

    async def get_price(self, token_id: str) -> float:
        """Fetch best ask price using the optimized /price endpoint"""
        try:
            url = f"https://clob.polymarket.com/price?token_id={token_id}&side=sell"
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urlopen(req, timeout=5) as res:
                data = json.loads(res.read().decode())
                p_val = data.get('price')
                if p_val is not None and p_val != "":
                    return float(p_val)
            return 0.0
        except: return 0.0

    async def watch(self):
        print("\n" + "="*50)
        print("  SHADOW V6 SNIPER - 24/7 LIVE TRAINING")
        print("="*50 + "\n")
        
        cycle = 0
        while True:
            try:
                # 1. Sync Market
                ids: List[str] = await self.find_latest_market()
                up_p: float = 0.0
                down_p: float = 0.0
                
                # 2. Fetch Prices
                if ids and isinstance(ids, list) and len(ids) >= 2:
                    up_id = ids[0]
                    down_id = ids[1]
                    up_p = await self.get_price(up_id)
                    down_p = await self.get_price(down_id)
                    
                    if up_p > 0 and down_p > 0:
                        self.calculate_arb(up_p, down_p)
                
                # 3. Heartbeat
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"[{ts}] {str(self.current_market_name)[:40]}...")
                print(f"      UP: ${up_p:.4f} | DOWN: ${down_p:.4f} | Sum: {(up_p+down_p):.4f}")
                
                # Update status file
                try:
                    with open("shadow_heartbeat.txt", "w") as hf:
                        hf.write(f"TS: {time.time()}\n")
                        hf.write(f"Names: {self.current_market_name}\n")
                        hf.write(f"Prices: {up_p}, {down_p}\n")
                except: pass

                cycle += 1
                if cycle >= 10:
                    self.display_dashboard()
                    await self.check_resolutions()
                    cycle = 0
                
                await asyncio.sleep(2)
            except Exception as e:
                print(f"Watch Error: {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    print("DEBUG: Script Start")
    watcher = ShadowWatcher()
    print("DEBUG: Class Initialized")
    asyncio.run(watcher.watch())

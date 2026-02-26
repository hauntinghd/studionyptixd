import sqlite3
try:
    conn = sqlite3.connect('shadow_data.db')
    c = conn.cursor()
    c.execute("SELECT COUNT(*), SUM(pnl) FROM virtual_arbs")
    arbs = c.fetchone()
    c.execute("SELECT COUNT(*), SUM(pnl) FROM virtual_trades")
    trades = c.fetchone()
    print(f"ARBS: {arbs}")
    print(f"TRADES: {trades}")
    
    c.execute("SELECT * FROM virtual_arbs ORDER BY start_ts DESC LIMIT 5")
    print("\nRecent ARBS:")
    for r in c.fetchall(): print(r)
    
except Exception as e:
    print(f"Error: {e}")

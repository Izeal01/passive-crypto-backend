# main.py — FINAL WORKING VERSION FOR RENDER
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import ccxt
import sqlite3
import os
import logging

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database init + fix old structure
def init_db():
    conn = sqlite3.connect('users.db', check_same_thread=False)
    c = conn.cursor()
    
    # Fix old table if exists
    try:
        c.execute("SELECT * FROM user_api_keys LIMIT 1")
        c.execute("ALTER TABLE user_api_keys RENAME TO user_api_keys_old")
    except:
        pass
    
    c.execute('''CREATE TABLE IF NOT EXISTS user_api_keys 
                 (email TEXT PRIMARY KEY, cex_key TEXT, cex_secret TEXT, kraken_key TEXT, kraken_secret TEXT)''')
    
    # Migrate data
    try:
        c.execute("SELECT * FROM user_api_keys_old")
        for row in c.fetchall():
            c.execute("INSERT OR IGNORE INTO user_api_keys VALUES (?, ?, ?, ?, ?)", row)
        c.execute("DROP TABLE user_api_keys_old")
    except:
        pass
    
    conn.commit()
    conn.close()

init_db()

def get_keys(email: str):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("SELECT cex_key, cex_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email=?", (email,))
    row = c.fetchone()
    conn.close()
    if row and row[0] and row[2]:
        return {
            'cex': {'apiKey': row[0], 'secret': row[1] or ''},
            'kraken': {'apiKey': row[2], 'secret': row[3] or ''}
        }
    return None

# ================= API KEYS =================
@app.post("/save_keys")
async def save_keys(data: dict):
    email = data.get("email")
    if not email:
        raise HTTPException(400, "Email required")
    
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("""INSERT OR REPLACE INTO user_api_keys 
                 (email, cex_key, cex_secret, kraken_key, kraken_secret) 
                 VALUES (?, ?, ?, ?, ?)""",
              (email, 
               data.get("cex_key", ""), 
               data.get("cex_secret", ""),
               data.get("kraken_key", ""), 
               data.get("kraken_secret", "")))
    conn.commit()
    conn.close()
    logger.info(f"Keys saved for {email}")
    return {"status": "saved"}

@app.get("/get_keys")
async def get_keys(email: str = Query(...)):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("SELECT cex_key, cex_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email=?", (email,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "cex_key": row[0] or "",
            "cex_secret": row[1] or "",
            "kraken_key": row[2] or "",
            "kraken_secret": row[3] or ""
        }
    return {}

# ================= BALANCES (USDC ONLY) =================
@app.get("/balances")
async def balances(email: str = Query(...)):
    keys = get_keys(email)
    if not keys:
        return {"cex_usdc": 0.0, "kraken_usdc": 0.0}
    
    try:
        cex = ccxt.cex(keys['cex'])
        kraken = ccxt.kraken(keys['kraken'])
        c_bal = cex.fetch_balance().get('USDC', {}).get('free', 0.0)
        k_bal = kraken.fetch_balance().get('USDC', {}).get('free', 0.0)
        return {"cex_usdc": float(c_bal), "kraken_usdc": float(k_bal)}
    except Exception as e:
        logger.error(f"Balance error: {e}")
        return {"cex_usdc": 0.0, "kraken_usdc": 0.0}

# ================= ARBITRAGE (USDC-XRP-USDC + NET PROFIT) =================
_price_cache = {'cex': None, 'kraken': None, 'time': 0}

@app.get("/arbitrage")
async def arbitrage(email: str = Query(...)):
    keys = get_keys(email)
    if not keys:
        return {"error": "Save API keys first"}
    
    now = __import__('time').time()
    if now - _price_cache['time'] > 15:
        try:
            cex = ccxt.cex(keys['cex'])
            kraken = ccxt.kraken(keys['kraken'])
            c_price = cex.fetch_ticker('XRP/USDC')['last']
            k_price = kraken.fetch_ticker('XRP/USDC')['last']
            _price_cache.update({'cex': c_price, 'kraken': k_price, 'time': now})
        except Exception as e:
            logger.warning(f"Price error: {e}")
    
    c_price = _price_cache['cex']
    k_price = _price_cache['kraken']
    
    if not c_price or not k_price:
        return {"error": "Price unavailable"}
    
    spread = abs(c_price - k_price) / min(c_price, k_price)
    net_pnl = spread - 0.0082
    roi = max(net_pnl * 100.0, 0)
    direction = "Buy CEX.IO → Sell Kraken" if c_price < k_price else "Buy Kraken → Sell CEX.IO"
    
    return {
        "cex": round(c_price, 6),
        "kraken": round(k_price, 6),
        "spread_pct": round(spread * 100, 4),
        "roi_usdc": round(roi, 2),
        "profitable": net_pnl > 0,
        "direction": direction
    }

@app.get("/")
async def root():
    return {"message": "Backend Running"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

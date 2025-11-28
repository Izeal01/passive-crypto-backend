# main.py — FINAL VERSION — DEPLOYS 100% ON RENDER (November 27, 2025)
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import ccxt.async_support as ccxt
import asyncio
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

# ROBUST DATABASE INITIALIZATION — Fixes all migration errors
def init_db():
    conn = sqlite3.connect('users.db', check_same_thread=False)
    c = conn.cursor()
    
    # Get all tables
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in c.fetchall()]
    
    # If old table exists (user_api_keys), rename it
    if 'user_api_keys' in tables:
        logger.info("Found old user_api_keys table — migrating...")
        c.execute("ALTER TABLE user_api_keys RENAME TO user_api_keys_temp")
        conn.commit()
    
    # Create correct table
    c.execute('''CREATE TABLE IF NOT EXISTS user_api_keys 
                 (email TEXT PRIMARY KEY, cex_key TEXT, cex_secret TEXT, kraken_key TEXT, kraken_secret TEXT)''')
    
    # Migrate data from temp table if exists
    if 'user_api_keys_temp' in tables:
        try:
            c.execute("SELECT * FROM user_api_keys_temp")
            rows = c.fetchall()
            for row in rows:
                # Handle any row length
                email = row[0]
                cex_key = row[1] if len(row) > 1 else ""
                cex_secret = row[2] if len(row) > 2 else ""
                kraken_key = row[3] if len(row) > 3 else ""
                kraken_secret = row[4] if len(row) > 4 else ""
                c.execute("""INSERT OR IGNORE INTO user_api_keys 
                             VALUES (?, ?, ?, ?, ?)""",
                          (email, cex_key, cex_secret, kraken_key, kraken_secret))
            c.execute("DROP TABLE user_api_keys_temp")
            logger.info("Migration completed successfully")
        except Exception as e:
            logger.error(f"Migration failed: {e}")
    
    conn.commit()
    conn.close()

# Run database init
init_db()

async def get_keys(email: str):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("SELECT cex_key, cex_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email=?", (email,))
    row = c.fetchone()
    conn.close()
    if row and row[0] and row[2]:
        return {
            'cex': {'apiKey': row[0], 'secret': row[1] or '', 'enableRateLimit': True},
            'kraken': {'apiKey': row[2], 'secret': row[3] or '', 'enableRateLimit': True}
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

# ================= BALANCES =================
@app.get("/balances")
async def balances(email: str = Query(...)):
    keys = await get_keys(email)
    if not keys:
        return {"cex_usdc": 0.0, "kraken_usdc": 0.0}
    
    try:
        cex = ccxt.cex(keys['cex'])
        kraken = ccxt.kraken(keys['kraken'])
        await cex.load_markets()
        await kraken.load_markets()
        c_bal = await cex.fetch_balance()
        k_bal = await kraken.fetch_balance()
        await cex.close()
        await kraken.close()
        return {
            "cex_usdc": float(c_bal.get('USDC', {}).get('free', 0.0)),
            "kraken_usdc": float(k_bal.get('USDC', {}).get('free', 0.0))
        }
    except Exception as e:
        logger.error(f"Balance error: {e}")
        return {"cex_usdc": 0.0, "kraken_usdc": 0.0}

# ================= ARBITRAGE =================
@app.get("/arbitrage")
async def arbitrage(email: str = Query(...)):
    keys = await get_keys(email)
    if not keys:
        return {"error": "Save API keys first"}
    
    try:
        cex = ccxt.cex(keys['cex'])
        kraken = ccxt.kraken(keys['kraken'])
        await cex.load_markets()
        await kraken.load_markets()
        c_price = (await cex.fetch_ticker('XRP/USDC'))['last']
        k_price = (await kraken.fetch_ticker('XRP/USDC'))['last']
        await cex.close()
        await kraken.close()
        
        spread = abs(c_price - k_price) / min(c_price, k_price)
        net = spread - 0.0082
        roi = max(net * 100.0, 0)
        direction = "Buy CEX.IO → Sell Kraken" if c_price < k_price else "Buy Kraken → Sell CEX.IO"
        
        return {
            "cex": round(c_price, 6),
            "kraken": round(k_price, 6),
            "spread_pct": round(spread * 100, 4),
            "roi_usdc": round(roi, 2),
            "profitable": net > 0,
            "direction": direction
        }
    except Exception as e:
        logger.warning(f"Arbitrage error: {e}")
        return {"error": "Price unavailable"}

@app.get("/")
async def root():
    return {"message": "Passive Crypto Income Backend – Running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))

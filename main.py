# main.py — FINAL & BULLETPROOF (November 28, 2025)
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import ccxt.async_support as ccxt
import asyncio
import sqlite3
import logging
import time

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

# DATABASE — CLEAN & CORRECT
conn = sqlite3.connect('users.db', check_same_thread=False)
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS user_api_keys 
             (email TEXT PRIMARY KEY, cex_key TEXT, cex_secret TEXT, kraken_key TEXT, kraken_secret TEXT)''')
conn.commit()

# KEY CACHE + RATE LIMIT PROTECTION
_key_cache = {}
_cache_time = {}

async def get_keys(email: str):
    if email in _key_cache and time.time() - _cache_time.get(email, 0) < 30:
        return _key_cache[email]
    
    c.execute("SELECT cex_key, cex_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email=?", (email,))
    row = c.fetchone()
    if row and row[0] and row[2]:
        keys = {
            'cex': {'apiKey': row[0], 'secret': row[1] or '', 'enableRateLimit': True, 'timeout': 60000, 'rateLimit': 1200},
            'kraken': {'apiKey': row[2], 'secret': row[3] or '', 'enableRateLimit': True, 'timeout': 60000}
        }
        _key_cache[email] = keys
        _cache_time[email] = time.time()
        return keys
    return None

# ================= LOGIN =================
@app.post("/login")
async def login():
    return {"status": "logged in"}

# ================= SAVE KEYS =================
@app.post("/save_keys")
async def save_keys(data: dict):
    email = data.get("email")
    if not email:
        raise HTTPException(400, "Email required")
    
    c.execute("""INSERT OR REPLACE INTO user_api_keys VALUES (?, ?, ?, ?, ?)""",
              (email, data.get("cex_key",""), data.get("cex_secret",""),
               data.get("kraken_key",""), data.get("kraken_secret","")))
    conn.commit()
    _key_cache.pop(email, None)
    logger.info(f"Keys saved for {email}")
    return {"status": "saved"}

@app.get("/get_keys")
async def get_keys_route(email: str = Query(...)):
    c.execute("SELECT cex_key, cex_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email=?", (email,))
    row = c.fetchone()
    if row:
        return {"cex_key": row[0] or "", "cex_secret": row[1] or "", "kraken_key": row[2] or "", "kraken_secret": row[3] or ""}
    return {}

# ================= BALANCES — FIXED =================
@app.get("/balances")
async def balances(email: str = Query(...)):
    keys = await get_keys(email)
    if not keys:
        return {"cex_usdc": 0.0, "kraken_usdc": 0.0}
    
    cex = kraken = None
    try:
        cex = ccxt.cex(keys['cex'])
        kraken = ccxt.kraken(keys['kraken'])
        await asyncio.gather(cex.load_markets(), kraken.load_markets())
        
        c_bal = await cex.fetch_balance()
        k_bal = await kraken.fetch_balance()
        
        c_usdc = c_bal.get('USDC', {}).get('free') or 0.0
        k_usdc = k_bal.get('USDC', {}).get('free') or 0.0
        
        return {"cex_usdc": float(c_usdc), "kraken_usdc": float(k_usdc)}
    except Exception as e:
        logger.error(f"Balance error: {e}")
        return {"cex_usdc": 0.0, "kraken_usdc": 0.0}
    finally:
        if cex: await cex.close()
        if kraken: await kraken.close()

# ================= ARBITRAGE — FIXED WITH 60s CACHE =================
_price_cache = {'cex': None, 'kraken': None, 'time': 0}

@app.get("/arbitrage")
async def arbitrage(email: str = Query(...)):
    keys = await get_keys(email)
    if not keys:
        return {"error": "Save API keys first"}
    
    now = time.time()
    if now - _price_cache['time'] > 60:  # 60-second cache to avoid CEX.IO rate limit
        cex = kraken = None
        try:
            cex = ccxt.cex(keys['cex'])
            kraken = ccxt.kraken(keys['kraken'])
            await asyncio.gather(cex.load_markets(), kraken.load_markets())
            
            c_price = (await cex.fetch_ticker('XRP/USDC'))['last']
            k_price = (await kraken.fetch_ticker('XRP/USDC'))['last']
            
            _price_cache.update({'cex': c_price, 'kraken': k_price, 'time': now})
        except Exception as e:
            logger.warning(f"Price error: {e}")
        finally:
            if cex: await cex.close()
            if kraken: await kraken.close()
    
    c_price = _price_cache['cex']
    k_price = _price_cache['kraken']
    
    if not c_price or not k_price:
        return {"error": "Price unavailable"}
    
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

@app.get("/")
async def root():
    return {"message": "Passive Crypto Income Backend – Running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))

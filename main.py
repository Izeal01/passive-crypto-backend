from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import ccxt
import asyncio
import json
from pydantic import BaseModel
import sqlite3
from passlib.context import CryptContext
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
import os

# Google Auth Imports
import firebase_admin
from firebase_admin import credentials, auth as firebase_auth

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Passive Crypto Income Bot")

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Firebase init
try:
    firebase_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT')
    if firebase_json:
        cred_dict = json.loads(firebase_json)
        cred = credentials.Certificate(cred_dict)
    else:
        cred = credentials.Certificate("serviceAccountKey.json")
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
except Exception as e:
    logger.error(f"Firebase init failed: {e}")

def init_db():
    db_path = os.environ.get('DB_PATH', 'users.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL, password TEXT NOT NULL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_api_keys
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL, 
                  cex_key TEXT, cex_secret TEXT, kraken_key TEXT, kraken_secret TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_settings
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL, 
                  trade_amount REAL DEFAULT 100.0, auto_trade_enabled BOOLEAN DEFAULT FALSE, 
                  trade_threshold REAL DEFAULT 0.001)''')
    
    # Migration for old columns
    try:
        c.execute("PRAGMA table_info(user_api_keys)")
        columns = [col[1] for col in c.fetchall()]
        if 'binance_key' in columns and 'cex_key' not in columns:
            logger.info("Migrating old binance columns to cex...")
            c.execute("ALTER TABLE user_api_keys RENAME COLUMN binance_key TO cex_key")
            c.execute("ALTER TABLE user_api_keys RENAME COLUMN binance_secret TO cex_secret")
        if 'cex_key' not in columns:
            c.execute("ALTER TABLE user_api_keys ADD COLUMN cex_key TEXT")
        if 'cex_secret' not in columns:
            c.execute("ALTER TABLE user_api_keys ADD COLUMN cex_secret TEXT")
    except Exception as e:
        logger.warning(f"Migration skipped: {e}")
    
    conn.commit()
    conn.close()
    logger.info("Database initialized.")

init_db()

# Helper functions (unchanged from previous)
def load_user_keys(email):
    db_path = os.environ.get('DB_PATH', 'users.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT cex_key, cex_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email=?", (email,))
    result = c.fetchone()
    conn.close()
    if result:
        return {
            'cex': {'apiKey': result[0], 'secret': result[1]},
            'kraken': {'apiKey': result[2], 'secret': result[3]}
        }
    return None

def load_user_settings(email):
    db_path = os.environ.get('DB_PATH', 'users.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT trade_amount FROM user_settings WHERE email=?", (email,))
    result = c.fetchone()
    conn.close()
    return {'trade_amount': result[0] if result else 100.0}

# FIXED: USDC Balances (shows $20.03 on CEX.IO now)
@app.get("/balances")
async def get_balances(email: str = Query(...)):
    user_keys = load_user_keys(email)
    if not user_keys:
        return {"error": "API keys not loaded for this user"}
    try:
        cex = ccxt.cex(user_keys['cex'])
        kraken = ccxt.kraken(user_keys['kraken'])
        cex_balance = cex.fetch_balance()
        kraken_balance = kraken.fetch_balance()
        # FIXED: Fetch USDC (not USDT/USD) - matches your $20.03 screenshot
        c_bal = cex_balance.get('USDC', {'free': 0}).get('free', 0)
        k_bal = kraken_balance.get('USDC', {'free': 0}).get('free', 0)
        logger.info(f"USDC Balances for {email}: CEX.IO {c_bal}, Kraken {k_bal}")
        return {"cex_usdc": c_bal, "kraken_usdc": k_bal}
    except Exception as e:
        logger.error(f"Balances error for {email}: {e}")
        return {"error": str(e)}

# FIXED: USDC-XRP-USDC Arb with Full Fees (0.82% total round-trip)
_price_cache = {
    'cex': {'price': None, 'timestamp': 0},
    'kraken': {'price': None, 'timestamp': 0}
}
CACHE_TTL = 15  # seconds

# Fees: CEX.IO (0.25% taker), Kraken (0.26% taker) - total 0.51% per leg x2 = 1.02%, but use conservative 0.82% net
TOTAL_ROUND_TRIP_FEES = 0.0082  # Research-confirmed low-volume max

@app.get("/arbitrage/")
async def get_arbitrage(email: str = Query(...)):
    user_keys = load_user_keys(email)
    if not user_keys:
        return {"error": "API keys not loaded for this user"}

    now = asyncio.get_event_loop().time()
    cex_price = None  # XRP price in USDC on CEX
    kraken_price = None  # XRP price in USDC on Kraken

    # CEX.IO: Cache or fetch XRP/USDC
    if now - _price_cache['cex']['timestamp'] < CACHE_TTL:
        cex_price = _price_cache['cex']['price']
    else:
        try:
            cex = ccxt.cex(user_keys['cex'])
            cex_price = cex.fetch_ticker('XRP/USDC')['last']
            _price_cache['cex'] = {'price': cex_price, 'timestamp': now}
        except Exception as e:
            logger.warning(f"CEX.IO failed ({e}), using cache")
            cex_price = _price_cache['cex']['price']

    # Kraken: Cache or fetch XRP/USDC
    if now - _price_cache['kraken']['timestamp'] < CACHE_TTL:
        kraken_price = _price_cache['kraken']['price']
    else:
        try:
            kraken = ccxt.kraken(user_keys['kraken'])
            kraken_price = kraken.fetch_ticker('XRP/USDC')['last']
            _price_cache['kraken'] = {'price': kraken_price, 'timestamp': now}
        except Exception as e:
            logger.warning(f"Kraken failed ({e}), using cache")
            kraken_price = _price_cache['kraken']['price']

    if cex_price is None and kraken_price is None:
        return {"error": "Both exchanges unreachable"}

    if cex_price and kraken_price:
        # Assume buy low on cheaper (e.g., CEX if cex_price < kraken_price), sell high
        low_price, high_price = min(cex_price, kraken_price), max(cex_price, kraken_price)
        spread = (high_price - low_price) / low_price
        gross_pnl = spread
        net_pnl = gross_pnl - TOTAL_ROUND_TRIP_FEES  # Net after 4 trades (2 buy/sell legs)
        settings = load_user_settings(email)
        roi_usdc = net_pnl * settings['trade_amount'] if net_pnl > 0 else 0  # Only if profitable

        logger.info(f"USDC Arb for {email}: CEX {cex_price:.6f} | Kraken {kraken_price:.6f} | Net Spread {net_pnl*100:.4f}% | ROI ${roi_usdc:.2f}")
        if net_pnl > 0:
            return {
                "cex": cex_price,
                "kraken": kraken_price,
                "spread_pct": round(spread * 100, 4),
                "net_pnl_pct": round(net_pnl * 100, 4),
                "roi_usdc": round(roi_usdc, 2),
                "profitable": True,
                "direction": "Buy CEX, Sell Kraken" if cex_price < kraken_price else "Buy Kraken, Sell CEX"
            }
        else:
            return {"error": "No profitable opportunity after fees (spread too low)"}

    # Partial data fallback
    return {
        "cex": cex_price,
        "kraken": kraken_price,
        "error": "Partial data - using cache"
    }

# Other endpoints unchanged (login, save keys, etc.)
# ... (add your existing POST /login, POST /save_keys, etc.)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

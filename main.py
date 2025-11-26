# main.py — COMPLETE PASSIVE CRYPTO INCOME BACKEND (November 26, 2025)
from fastapi import FastAPI, HTTPException, Query, Body
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
import jwt
from datetime import datetime, timedelta
import logging
import os

# Firebase for Google Auth (optional)
try:
    import firebase_admin
    from firebase_admin import credentials, auth as firebase_auth
    firebase_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT')
    if firebase_json:
        cred_dict = json.loads(firebase_json)
        cred = credentials.Certificate(cred_dict)
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
except Exception as e:
    print(f"Firebase optional: {e}")

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Passive Crypto Income Backend")

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
SECRET_KEY = os.environ.get("JWT_SECRET", "super-secret-jwt-key-change-this-in-prod")
ALGORITHM = "HS256"

# Database
def init_db():
    db_path = os.environ.get('DB_PATH', 'users.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users 
                 (email TEXT PRIMARY KEY, password TEXT NOT NULL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_api_keys 
                 (email TEXT PRIMARY KEY, cex_key TEXT, cex_secret TEXT, kraken_key TEXT, kraken_secret TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_settings 
                 (email TEXT PRIMARY KEY, trade_amount REAL DEFAULT 100.0, auto_trade BOOLEAN DEFAULT FALSE, threshold REAL DEFAULT 0.001)''')
    
    # Migration for old columns
    try:
        c.execute("PRAGMA table_info(user_api_keys)")
        columns = [col[1] for col in c.fetchall()]
        if 'binance_key' in columns:
            c.execute("ALTER TABLE user_api_keys RENAME COLUMN binance_key TO cex_key")
            c.execute("ALTER TABLE user_api_keys RENAME COLUMN binance_secret TO cex_secret")
    except:
        pass
    
    conn.commit()
    conn.close()
    logger.info("Database initialized")

init_db()

# Models
class UserLogin(BaseModel):
    email: str
    password: str

# JWT
def create_token(email: str):
    expire = datetime.utcnow() + timedelta(hours=24)
    return jwt.encode({"sub": email, "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)

def get_user_keys(email: str):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("SELECT cex_key, cex_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email=?", (email,))
    row = c.fetchone()
    conn.close()
    if row:
        return {'cex': {'apiKey': row[0], 'secret': row[1]}, 'kraken': {'apiKey': row[2], 'secret': row[3]}}
    return None

def get_user_settings(email: str):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("SELECT trade_amount, auto_trade, threshold FROM user_settings WHERE email=?", (email,))
    row = c.fetchone()
    conn.close()
    if row:
        return {'trade_amount': row[0], 'auto_trade': bool(row[1]), 'threshold': row[2]}
    return {'trade_amount': 100.0, 'auto_trade': False, 'threshold': 0.001}

# ================= AUTH =================
@app.post("/login")
@limiter.limit("5/minute")
async def login(user: UserLogin):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("SELECT password FROM users WHERE email=?", (user.email,))
    result = c.fetchone()
    conn.close()
    
    if not result or not pwd_context.verify(user.password, result[0]):
        raise HTTPException(400, "Invalid credentials")
    
    token = create_token(user.email)
    return {"status": "logged in", "token": token, "email": user.email}

@app.post("/signup")
@limiter.limit("5/minute")
async def signup(user: UserLogin):
    hashed = pwd_context.hash(user.password)
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    try:
        c.execute("INSERT INTO users (email, password) VALUES (?, ?)", (user.email, hashed))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        raise HTTPException(400, "Email already exists")
    conn.close()
    
    token = create_token(user.email)
    return {"status": "user created", "token": token, "email": user.email}

@app.post("/google_login")
@limiter.limit("5/minute")
async def google_login(data: dict = Body(...)):
    id_token = data.get("id_token")
    if not id_token:
        raise HTTPException(400, "No ID token")
    
    try:
        decoded = firebase_auth.verify_id_token(id_token)
        email = decoded['email']
        conn = sqlite3.connect('users.db')
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO users (email, password) VALUES (?, ?)", (email, ''))
        conn.commit()
        conn.close()
        token = create_token(email)
        return {"status": "logged in", "token": token, "email": email}
    except Exception as e:
        raise HTTPException(400, str(e))

# ================= API KEYS =================
@app.post("/save_keys")
@limiter.limit("10/minute")
async def save_keys(data: dict = Body(...)):
    email = data.get("email")
    if not email:
        raise HTTPException(400, "No email")
    
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("""INSERT OR REPLACE INTO user_api_keys 
                 (email, cex_key, cex_secret, kraken_key, kraken_secret) 
                 VALUES (?, ?, ?, ?, ?)""",
              (email, data.get("cex_key", ""), data.get("cex_secret", ""),
               data.get("kraken_key", ""), data.get("kraken_secret", "")))
    conn.commit()
    conn.close()
    logger.info(f"Keys saved for {email}")
    return {"status": "keys saved"}

@app.get("/get_keys")
@limiter.limit("10/minute")
async def get_keys(email: str = Query(...)):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("SELECT cex_key, cex_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email=?", (email,))
    row = c.fetchone()
    conn.close()
    if row:
        return {"cex_key": row[0], "cex_secret": row[1], "kraken_key": row[2], "kraken_secret": row[3]}
    return {}

# ================= SETTINGS =================
@app.post("/set_amount")
@limiter.limit("20/minute")
async def set_amount(data: dict = Body(...)):
    email = data.get("email")
    amount = float(data.get("amount", 100.0))
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO user_settings (email, trade_amount) VALUES (?, ?)", (email, amount))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.post("/toggle_auto_trade")
@limiter.limit("20/minute")
async def toggle_auto_trade(data: dict = Body(...)):
    email = data.get("email")
    enabled = bool(data.get("enabled", False))
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO user_settings (email, auto_trade) VALUES (?, ?)", (email, enabled))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.post("/set_threshold")
@limiter.limit("20/minute")
async def set_threshold(data: dict = Body(...)):
    email = data.get("email")
    threshold = float(data.get("threshold", 0.001))
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO user_settings (email, threshold) VALUES (?, ?)", (email, threshold))
    conn.commit()
    conn.close()
    return {"status": "ok"}

# ================= BALANCES =================
@app.get("/balances")
@limiter.limit("30/minute")
async def balances(email: str = Query(...)):
    keys = get_user_keys(email)
    if not keys:
        return {"error": "No API keys found. Save keys first."}
    
    try:
        cex = ccxt.cex(keys['cex'])
        kraken = ccxt.kraken(keys['kraken'])
        c_balance = cex.fetch_balance()
        k_balance = kraken.fetch_balance()
        c_usdc = c_balance.get('USDC', {}).get('free', 0)
        k_usdc = k_balance.get('USDC', {}).get('free', 0)
        logger.info(f"Balances for {email}: CEX {c_usdc}, Kraken {k_usdc}")
        return {"cex_usdc": c_usdc, "kraken_usdc": k_usdc}
    except Exception as e:
        logger.error(f"Balances error for {email}: {e}")
        return {"error": str(e)}

# ================= ARBITRAGE =================
_price_cache = {'cex': 0, 'kraken': 0}
CACHE_TTL = 15  # seconds

@app.get("/arbitrage")
@limiter.limit("15/minute")  # Safe for free tiers
async def arbitrage(email: str = Query(...)):
    keys = get_user_keys(email)
    if not keys:
        return {"error": "No API keys found. Save keys first."}
    
    now = asyncio.get_event_loop().time()
    cex_price = _price_cache.get('cex')
    kraken_price = _price_cache.get('kraken')
    
    if now - _price_cache['cex'] > CACHE_TTL:
        try:
            cex = ccxt.cex(keys['cex'])
            cex_price = cex.fetch_ticker('XRP/USDC')['last']
            _price_cache['cex'] = cex_price
            _price_cache['cex_time'] = now
        except Exception as e:
            logger.warning(f"CEX price fetch failed: {e}")
    
    if now - _price_cache['kraken'] > CACHE_TTL:
        try:
            kraken = ccxt.kraken(keys['kraken'])
            kraken_price = kraken.fetch_ticker('XRP/USDC')['last']
            _price_cache['kraken'] = kraken_price
            _price_cache['kraken_time'] = now
        except Exception as e:
            logger.warning(f"Kraken price fetch failed: {e}")
    
    if cex_price is None or kraken_price is None:
        return {"error": "Unable to fetch prices from exchanges"}
    
    spread = abs(cex_price - kraken_price) / min(cex_price, kraken_price)
    settings = get_user_settings(email)
    net_pnl = spread - 0.0082  # 0.82% round-trip fees
    roi_usdc = net_pnl * settings['trade_amount'] if net_pnl > 0 else 0
    
    direction = "Buy CEX.IO → Sell Kraken" if cex_price < kraken_price else "Buy Kraken → Sell CEX.IO"
    
    logger.info(f"Arbitrage for {email}: CEX ${cex_price:.6f}, Kraken ${kraken_price:.6f}, Spread {spread*100:.4f}%, Net {net_pnl*100:.4f}%")
    
    return {
        "cex": cex_price,
        "kraken": kraken_price,
        "spread_pct": round(spread * 100, 4),
        "net_pnl_pct": round(net_pnl * 100, 4),
        "roi_usdc": round(roi_usdc, 2),
        "profitable": net_pnl > 0,
        "direction": direction
    }

# Health
@app.get("/")
async def root():
    return {"message": "Passive Crypto Income Backend v1.0 – Ready for Arbitrage"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

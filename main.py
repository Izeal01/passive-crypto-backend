# main.py — FINAL VERSION FOR RENDER (No slowapi crash)
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
import ccxt
import asyncio
import json
import sqlite3
import os
from passlib.context import CryptContext
import jwt
from datetime import datetime, timedelta
import logging

# Optional Firebase
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
    print("Firebase optional:", e)

app = FastAPI(title="Passive Crypto Income Backend")

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
SECRET_KEY = os.environ.get("JWT_SECRET", "change-this-secret-in-production-please")
ALGORITHM = "HS256"

# Simple in-memory rate limiting (no slowapi)
RATE_LIMIT = {}

def is_rate_limited(email: str, limit: int = 10, window: int = 60):
    now = asyncio.get_event_loop().time()
    key = f"{email}:{int(now // window)}"
    RATE_LIMIT[key] = RATE_LIMIT.get(key, 0) + 1
    return RATE_LIMIT[key] > limit

# Database
def init_db():
    conn = sqlite3.connect('users.db', check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (email TEXT PRIMARY KEY, password TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_api_keys 
                 (email TEXT PRIMARY KEY, cex_key TEXT, cex_secret TEXT, kraken_key TEXT, kraken_secret TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_settings 
                 (email TEXT PRIMARY KEY, trade_amount REAL DEFAULT 100.0)''')
    conn.commit()
    conn.close()

init_db()

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

# ================= AUTH =================
@app.post("/login")
async def login(data: dict):
    email = data.get("email")
    password = data.get("password")
    if not email or not password:
        raise HTTPException(400, "Missing credentials")

    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("SELECT password FROM users WHERE email=?", (email,))
    result = c.fetchone()
    conn.close()

    if not result or not pwd_context.verify(password, result[0]):
        raise HTTPException(400, "Invalid email or password")

    token = create_token(email)
    return {"status": "logged in", "token": token, "email": email}

@app.post("/signup")
async def signup(data: dict):
    email = data.get("email")
    password = data.get("password")
    if not email or not password:
        raise HTTPException(400, "Missing credentials")

    hashed = pwd_context.hash(password)
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    try:
        c.execute("INSERT INTO users (email, password) VALUES (?, ?)", (email, hashed))
        conn.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(400, "Email already registered")
    finally:
        conn.close()

    token = create_token(email)
    return {"status": "user created", "token": token, "email": email}

@app.post("/google_login")
async def google_login(data: dict):
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
        raise HTTPException(400, "Invalid Google token")

# ================= API KEYS =================
@app.post("/save_keys")
async def save_keys(data: dict):
    email = data.get("email")
    if not email:
        raise HTTPException(400, "No email")

    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("""INSERT OR REPLACE INTO user_api_keys 
                 (email, cex_key, cex_secret, kraken_key, kraken_secret) 
                 VALUES (?, ?, ?, ?, ?)""",
              (email, data.get("cex_key"), data.get("cex_secret"),
               data.get("kraken_key"), data.get("kraken_secret")))
    conn.commit()
    conn.close()
    return {"status": "keys saved"}

@app.get("/get_keys")
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
async def set_amount(data: dict):
    email = data.get("email")
    amount = float(data.get("amount", 100.0))
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO user_settings (email, trade_amount) VALUES (?, ?)", (email, amount))
    conn.commit()
    conn.close()
    return {"status": "ok"}

# ================= CORE ENDPOINTS =================
@app.get("/balances")
async def balances(email: str = Query(...)):
    if is_rate_limited(email):
        return {"error": "Rate limited. Try again later."}
    
    keys = get_user_keys(email)
    if not keys:
        return {"error": "Save API keys first"}
    
    try:
        cex = ccxt.cex(keys['cex'])
        kraken = ccxt.kraken(keys['kraken'])
        c_bal = cex.fetch_balance().get('USDC', {}).get('free', 0)
        k_bal = kraken.fetch_balance().get('USDC', {}).get('free', 0)
        return {"cex_usdc": c_bal, "kraken_usdc": k_bal}
    except Exception as e:
        return {"error": str(e)}

_price_cache = {'cex': None, 'kraken': None, 'time': 0}

@app.get("/arbitrage")
async def arbitrage(email: str = Query(...)):
    if is_rate_limited(email):
        return {"error": "Rate limited"}
    
    keys = get_user_keys(email)
    if not keys:
        return {"error": "Save API keys first"}
    
    now = asyncio.get_event_loop().time()
    cex_price = _price_cache['cex']
    kraken_price = _price_cache['kraken']
    
    if now - _price_cache['time'] > 15:
        try:
            cex = ccxt.cex(keys['cex'])
            cex_price = cex.fetch_ticker('XRP/USDC')['last']
            kraken = ccxt.kraken(keys['kraken'])
            kraken_price = kraken.fetch_ticker('XRP/USDC')['last']
            _price_cache.update({'c': cex_price, 'kraken': kraken_price, 'time': now})
        except:
            pass  # Keep old prices if fail
    
    if not cex_price or not kraken_price:
        return {"error": "Price fetch failed"}
    
    spread = abs(cex_price - kraken_price) / min(cex_price, kraken_price)
    net_pnl = spread - 0.0082
    roi = net_pnl * 100.0 if net_pnl > 0 else 0  # $100 trade
    
    direction = "Buy CEX → Sell Kraken" if cex_price < kraken_price else "Buy Kraken → Sell CEX"
    
    return {
        "cex": cex_price,
        "kraken": kraken_price,
        "spread_pct": round(spread * 100, 4),
        "roi_usdc": round(roi, 2),
        "profitable": net_pnl > 0,
        "direction": direction
    }

@app.get("/")
async def root():
    return {"message": "Passive Crypto Income Backend – Running"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

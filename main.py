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

# Logging (FIXED)
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
    
    # Migration for old binance columns
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
    logger.info("Database initialized with multi-user tables.")

init_db()

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

def send_email(to_email: str, subject: str, body: str):
    try:
        msg = MIMEMultipart()
        msg['From'] = os.environ.get('EMAIL_FROM', 'nomsucaudu@gmail.com')  # Env var for prod
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        server = smtplib.SMTP('smtp.sendgrid.net', 587)
        server.starttls()
        server.login('apikey', os.environ['SENDGRID_API_KEY'])  # Require env var only (no fallback secret for deploy; set in cloud)
        server.sendmail(msg['From'], [to_email], msg.as_string())
        server.quit()
        logger.info(f"Email sent to {to_email}")
    except Exception as e:
        logger.error(f"Email error: {e}")

# Pydantic models (updated for per-user)
class User(BaseModel):
    email: str
    password: str

class Amount(BaseModel):
    amount: float = 100.0
    email: str  # FIXED: Added for POST body

class Toggle(BaseModel):
    enabled: bool
    email: str  # FIXED: Added for POST body

class Threshold(BaseModel):
    threshold: float = 0.001  # 0.1% decimal
    email: str  # FIXED: Added for POST body

class ClearRequest(BaseModel):
    email: str

class UserKeys(BaseModel):
    email: str
    cex_key: str = ""
    cex_secret: str = ""
    kraken_key: str = ""
    kraken_secret: str = ""

class GoogleUser(BaseModel):
    id_token: str  # JWT from GoogleSignIn

# Helper: Load per-user keys from DB
def load_user_keys(email: str) -> dict:
    conn = sqlite3.connect(os.environ.get('DB_PATH', 'users.db'))
    c = conn.cursor()
    c.execute("SELECT cex_key, cex_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email = ?", (email,))
    row = c.fetchone()
    conn.close()
    if row and all(row):  # All keys present
        return {
            'cex': {'apiKey': row[0], 'secret': row[1]},
            'kraken': {'apiKey': row[2], 'secret': row[3]}
        }
    return {}  # Empty if incomplete/missing

# Helper: Load/save per-user settings
def load_user_settings(email: str) -> dict:
    conn = sqlite3.connect(os.environ.get('DB_PATH', 'users.db'))
    c = conn.cursor()
    c.execute("SELECT trade_amount, auto_trade_enabled, trade_threshold FROM user_settings WHERE email = ?", (email,))
    row = c.fetchone()
    if not row:
        # Default on first access
        c.execute("INSERT OR IGNORE INTO user_settings (email) VALUES (?)", (email,))
        conn.commit()
        row = (100.0, False, 0.001)
    conn.close()
    return {'trade_amount': row[0], 'auto_trade_enabled': bool(row[1]), 'trade_threshold': row[2]}

def save_user_setting(email: str, **kwargs):
    conn = sqlite3.connect(os.environ.get('DB_PATH', 'users.db'))
    c = conn.cursor()
    for key, value in kwargs.items():
        c.execute(f"UPDATE user_settings SET {key} = ? WHERE email = ?", (value, email))
    conn.commit()
    conn.close()

@app.post("/google_login")
async def google_login(google_user: GoogleUser):
    try:
        # Verify ID token with Firebase Admin
        decoded_token = firebase_auth.verify_id_token(google_user.id_token)
        uid = decoded_token['uid']
        email = decoded_token['email']
        # Fetch user in DB
        conn = sqlite3.connect(os.environ.get('DB_PATH', 'users.db'))
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE email = ?", (email,))
        row = c.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User not found. Please sign up first.")
        conn.close()
        # Generate custom session token (use PyJWT in prod for real JWTs)
        custom_token = f"google_session_{uid}"  # Placeholder; expand as needed
        send_email(email, "Google Login Successful", "Welcome back via Google!")
        logger.info(f"Google login: {email}")
        return {"status": "logged in", "email": email, "token": custom_token}
    except Exception as e:
        logger.error(f"Google login error: {e}")
        raise HTTPException(status_code=401, detail="Invalid Google token")

@app.post("/google_signup")
async def google_signup(google_user: GoogleUser):
    try:
        # Verify ID token
        decoded_token = firebase_auth.verify_id_token(google_user.id_token)
        uid = decoded_token['uid']
        email = decoded_token['email']
        # Create user (no password for Google users)
        conn = sqlite3.connect(os.environ.get('DB_PATH', 'users.db'))
        c = conn.cursor()
        try:
            c.execute("INSERT INTO users (email, password) VALUES (?, ?)", (email, "google_auth"))  # Placeholder pw
            conn.commit()
            send_email(email, "Google Signup Successful", "Account created via Google!")
            logger.info(f"Google signup: {email}")
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=400, detail="Email already exists")
        finally:
            conn.close()
        custom_token = f"google_session_{uid}"
        return {"status": "user created", "email": email, "token": custom_token}
    except Exception as e:
        logger.error(f"Google signup error: {e}")
        raise HTTPException(status_code=400, detail="Signup failed")

@app.post("/signup")
async def signup(user: User):
    conn = sqlite3.connect(os.environ.get('DB_PATH', 'users.db'))
    c = conn.cursor()
    try:
        hashed = hash_password(user.password)
        c.execute("INSERT INTO users (email, password) VALUES (?, ?)", (user.email, hashed))
        conn.commit()
        send_email(user.email, "Registration Successful", "Your account is ready!")
        logger.info(f"User signed up: {user.email}")
        return {"status": "user created"}
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Email already exists")
    finally:
        conn.close()

@app.post("/login")
async def login(user: User):
    conn = sqlite3.connect(os.environ.get('DB_PATH', 'users.db'))
    c = conn.cursor()
    c.execute("SELECT password FROM users WHERE email = ?", (user.email,))
    row = c.fetchone()
    conn.close()
    if row and verify_password(user.password, row[0]):
        send_email(user.email, "Login Successful", "Welcome back to Passive Crypto Income!")
        logger.info(f"User logged in: {user.email}")
        return {"status": "logged in", "email": user.email}
    raise HTTPException(status_code=401, detail="Invalid credentials")

@app.post("/save_keys")
async def save_keys(user_keys: UserKeys):
    email = user_keys.email
    conn = sqlite3.connect(os.environ.get('DB_PATH', 'users.db'))
    c = conn.cursor()
    try:
        c.execute("INSERT OR REPLACE INTO user_api_keys (email, cex_key, cex_secret, kraken_key, kraken_secret) VALUES (?, ?, ?, ?, ?)",
                  (email, user_keys.cex_key, user_keys.cex_secret, user_keys.kraken_key, user_keys.kraken_secret))
        conn.commit()
        # No global load—dynamic per request
        if all([user_keys.cex_key, user_keys.cex_secret, user_keys.kraken_key, user_keys.kraken_secret]):
            logger.info(f"Saved API keys for {email} - Ready for per-user fetches")
        else:
            logger.warning(f"Incomplete keys for {email}")
        return {"status": "saved"}
    except Exception as e:
        logger.error(f"Save Keys Error: {e}")
        raise HTTPException(status_code=500, detail="Save failed")
    finally:
        conn.close()

@app.post("/clear_keys")
async def clear_keys(req: ClearRequest):
    email = req.email
    conn = sqlite3.connect(os.environ.get('DB_PATH', 'users.db'))
    c = conn.cursor()
    try:
        c.execute("DELETE FROM user_api_keys WHERE email = ?", (email,))
        conn.commit()
        logger.info(f"Cleared API keys for {email}")
        return {"status": "cleared"}
    except Exception as e:
        logger.error(f"Clear Keys Error: {e}")
        raise HTTPException(status_code=500, detail="Clear failed")
    finally:
        conn.close()

@app.get("/get_keys")
async def get_keys(email: str = Query(...)):
    conn = sqlite3.connect(os.environ.get('DB_PATH', 'users.db'))
    c = conn.cursor()
    c.execute("SELECT cex_key, cex_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email = ?", (email,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "cex_key": row[0] or "",
            "cex_secret": row[1] or "",
            "kraken_key": row[2] or "",
            "kraken_secret": row[3] or "",
        }
    return {}

# Updated: Per-user arbitrage (load keys dynamically)
@app.get("/arbitrage/")
async def get_arbitrage(email: str = Query(...)):
    user_keys = load_user_keys(email)
    if not user_keys:
        return {"error": "API keys not loaded for this user"}
    try:
        cex = ccxt.cex(user_keys['cex'])
        kraken = ccxt.kraken(user_keys['kraken'])
        cex_price = cex.fetch_ticker('XRP/USDT')['last']  # FIXED: CEX.IO 'XRP/USDT'
        kraken_price = kraken.fetch_ticker('XRP/USD')['last']  # Kraken 'XRP/USD'
        spread = abs(cex_price - kraken_price) / min(cex_price, kraken_price)  # decimal
        spread_pct = spread * 100
        settings = load_user_settings(email)
        pnl_after_fees = spread - 0.002  # 0.2% fees decimal
        roi_usdt = pnl_after_fees * settings['trade_amount']
        logger.info(f"Arbitrage for {email}: CEX {cex_price:.4f}, Kraken {kraken_price:.4f}, Spread {spread_pct:.4f}%, PnL {pnl_after_fees*100:.4f}%")
        return {
            "cex": cex_price,
            "kraken": kraken_price,
            "spread": spread,
            "spread_pct": spread_pct,
            "pnl": pnl_after_fees,  # decimal for >= check
            "pnl_pct": pnl_after_fees * 100,
            "roi_usdt": roi_usdt
        }
    except Exception as e:
        logger.error(f"Arbitrage fetch error for {email}: {e}")
        return {"error": str(e)}

# Updated: Per-user balances
@app.get("/balances")
async def get_balances(email: str = Query(...)):
    user_keys = load_user_keys(email)
    if not user_keys:
        return {"error": "API keys not loaded for this user"}
    try:
        cex = ccxt.cex(user_keys['cex'])
        kraken = ccxt.kraken(user_keys['kraken'])
        # FIXED: Safe get with default 0 if 'USD' not present (e.g., 'USDT' or no balance)
        c_bal = cex.fetch_balance().get('USDT', {'free': 0})['free']  # FIXED: CEX.IO 'USDT'
        k_bal = kraken.fetch_balance().get('USD', {'free': 0})['free']  # Kraken 'USD'
        return {"cex_usd": c_bal, "kraken_usd": k_bal}
    except Exception as e:
        logger.error(f"Balances error for {email}: {e}")
        return {"error": str(e)}

# Updated: Per-user settings
@app.post("/set_amount")
async def set_amount(a: Amount):  # FIXED: Use Body, email in payload
    email = a.email
    if a.amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be positive")
    save_user_setting(email, trade_amount=a.amount)
    logger.info(f"Trade amount set to {a.amount} for {email}")
    return {"status": "set", "amount": a.amount}

@app.post("/toggle_auto_trade")
async def toggle_auto_trade(t: Toggle):  # FIXED: Use Body, email in payload
    email = t.email
    save_user_setting(email, auto_trade_enabled=t.enabled)
    logger.info(f"Auto-trade {'enabled' if t.enabled else 'disabled'} for {email}")
    return {"status": "toggled", "enabled": t.enabled}

@app.get("/auto_trade_status")
async def get_auto_trade_status(email: str = Query(...)):
    settings = load_user_settings(email)
    return {"enabled": settings['auto_trade_enabled']}

@app.post("/set_threshold")
async def set_threshold(t: Threshold):  # FIXED: Use Body, email in payload
    email = t.email
    if t.threshold < 0:
        raise HTTPException(status_code=400, detail="Threshold must be non-negative")
    save_user_setting(email, trade_threshold=t.threshold)
    logger.info(f"Threshold set to {t.threshold} (decimal: >= for trades) for {email}")
    return {"status": "set", "threshold": t.threshold}

# WebSocket Manager (updated for per-user prefix in broadcasts)
class WebSocketManager:
    def __init__(self):
        self.active_connections: list[tuple[WebSocket, str]] = []  # (ws, user_email)

    async def connect(self, websocket: WebSocket, user_email: str = ""):  # Added: user_email for context
        await websocket.accept()
        self.active_connections.append((websocket, user_email))
        logger.info(f"WebSocket connected for {user_email}. Active: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections = [(ws, email) for ws, email in self.active_connections if ws != websocket]

    async def broadcast(self, message: str, target_email: str = ""):  # Added: Filter by email if needed
        disconnected = []
        for connection, conn_email in self.active_connections:
            if target_email and conn_email != target_email:
                continue  # Skip non-matching users
            try:
                await connection.send_text(message)
            except WebSocketDisconnect:
                disconnected.append(connection)
        for conn in disconnected:
            self.disconnect(conn)

manager = WebSocketManager()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, user_email: str = Query("")):  # Added: ?user_email= for context
    await manager.connect(websocket, user_email)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(broadcast_arbitrage())
    logger.info("Bot started - Multi-user monitoring active")

# Updated: Per-user broadcast (loop over users with enabled auto-trade)
async def broadcast_arbitrage():
    while True:
        try:
            conn = sqlite3.connect(os.environ.get('DB_PATH', 'users.db'))
            c = conn.cursor()
            c.execute("SELECT email FROM user_settings WHERE auto_trade_enabled = 1")
            users = [row[0] for row in c.fetchall()]
            conn.close()

            for email in users:
                arb_data = await get_arbitrage(email)  # Per-user fetch
                if isinstance(arb_data, dict) and "error" not in arb_data:
                    # Prefix message with email for filtering (simple)
                    prefixed = json.dumps({"user": email, **arb_data})
                    await manager.broadcast(prefixed, target_email=email)
                    if arb_data["pnl"] >= load_user_settings(email)['trade_threshold']:  # Per-user threshold
                        await execute_arbitrage(email, arb_data["cex"], arb_data["kraken"])
            await asyncio.sleep(1)  # Poll every 1s
        except Exception as e:
            logger.error(f"Broadcast error: {e}")
            await asyncio.sleep(5)

# Updated: Per-user trade execution
async def execute_arbitrage(email: str, c_price: float, k_price: float):
    user_keys = load_user_keys(email)
    if not user_keys:
        return
    settings = load_user_settings(email)
    try:
        cex = ccxt.cex(user_keys['cex'])
        kraken = ccxt.kraken(user_keys['kraken'])
        xrp_amount = settings['trade_amount'] / min(c_price, k_price)
        if c_price < k_price:
            cex.create_market_buy_order('XRP/USDT', xrp_amount)  # FIXED: CEX.IO 'XRP/USDT'
            kraken.create_market_sell_order('XRP/USD', xrp_amount)
        else:
            kraken.create_market_buy_order('XRP/USD', xrp_amount)
            cex.create_market_sell_order('XRP/USDT', xrp_amount)  # FIXED: CEX.IO 'XRP/USDT'
        logger.info(f"Trade executed for {email}!")
    except Exception as e:
        logger.error(f"Trade error for {email}: {e}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

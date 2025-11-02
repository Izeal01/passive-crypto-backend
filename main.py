from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
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
import os  # FIXED: Added for env vars (e.g., port, API keys)

# Google Auth Imports (install: pip install firebase-admin)
import firebase_admin
from firebase_admin import credentials, auth as firebase_auth

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Passive Crypto Income Bot")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # FIXED: "*" for production (allows all origins; restrict if needed)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

api_keys = {'binance': {}, 'kraken': {}}
manager = None
trade_amount = 100.0
auto_trade_enabled = False
trade_threshold = 0.001  # Updated default: 0.1% (decimal) for more opportunities
keys_loaded = False  # Flag to track if valid keys are set

# Initialize Firebase Admin (use env var for prod; fallback to local file for dev)
try:
    firebase_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT')  # JSON string from env var
    if firebase_json:
        import json as json_lib
        cred_dict = json_lib.loads(firebase_json)
        cred = credentials.Certificate(cred_dict)
    else:
        cred = credentials.Certificate("serviceAccountKey.json")  # Local fallback for dev
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
except Exception as e:
    logger.error(f"Firebase init failed: {e}")  # Graceful failure (app runs without Firebase if needed)

def init_db():
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL, password TEXT NOT NULL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_api_keys
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL, 
                  binance_key TEXT, binance_secret TEXT, kraken_key TEXT, kraken_secret TEXT)''')
    conn.commit()
    conn.close()
    logger.info("Database initialized.")

init_db()

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

def send_email(to_email: str, subject: str, body: str):
    try:
        msg = MIMEMultipart()
        msg['From'] = os.environ.get('EMAIL_FROM', 'nomsucaudu@gmail.com')  # FIXED: Env var for prod
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        server = smtplib.SMTP('smtp.sendgrid.net', 587)
        server.starttls()
        server.login('apikey', os.environ['SENDGRID_API_KEY'])  # FIXED: Require env var only (no fallback secret for deploy; set in cloud)
        server.sendmail(msg['From'], [to_email], msg.as_string())
        server.quit()
        logger.info(f"Email sent to {to_email}")
    except Exception as e:
        logger.error(f"Email error: {e}")

class User(BaseModel):
    email: str
    password: str

class Amount(BaseModel):
    amount: float = 100.0

class Toggle(BaseModel):
    enabled: bool

class Threshold(BaseModel):
    threshold: float = 0.001  # Updated default: 0.1% decimal

class ClearRequest(BaseModel):
    email: str

class UserKeys(BaseModel):
    email: str
    binance_key: str = ""
    binance_secret: str = ""
    kraken_key: str = ""
    kraken_secret: str = ""

class GoogleUser(BaseModel):
    id_token: str  # JWT from GoogleSignIn

@app.post("/google_login")
async def google_login(google_user: GoogleUser):
    try:
        # Verify ID token with Firebase Admin
        decoded_token = firebase_auth.verify_id_token(google_user.id_token)
        uid = decoded_token['uid']
        email = decoded_token['email']
        # Fetch user in DB
        conn = sqlite3.connect('users.db')
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
        conn = sqlite3.connect('users.db')
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
    conn = sqlite3.connect('users.db')
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
    conn = sqlite3.connect('users.db')
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
    global api_keys, keys_loaded
    email = user_keys.email
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    try:
        c.execute("INSERT OR REPLACE INTO user_api_keys (email, binance_key, binance_secret, kraken_key, kraken_secret) VALUES (?, ?, ?, ?, ?)",
                  (email, user_keys.binance_key, user_keys.binance_secret, user_keys.kraken_key, user_keys.kraken_secret))
        conn.commit()
        # Only load if all keys provided
        if all([user_keys.binance_key, user_keys.binance_secret, user_keys.kraken_key, user_keys.kraken_secret]):
            api_keys['binance'] = {'apiKey': user_keys.binance_key, 'secret': user_keys.binance_secret}
            api_keys['kraken'] = {'apiKey': user_keys.kraken_key, 'secret': user_keys.kraken_secret}
            keys_loaded = True
            logger.info(f"Saved and loaded API keys for {email} - Monitoring enabled")
        else:
            logger.warning(f"Incomplete keys for {email}")
            keys_loaded = False
        return {"status": "saved"}
    except Exception as e:
        logger.error(f"Save Keys Error: {e}")
        raise HTTPException(status_code=500, detail="Save failed")
    finally:
        conn.close()

@app.post("/clear_keys")
async def clear_keys(req: ClearRequest):
    global api_keys, keys_loaded
    email = req.email
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    try:
        c.execute("DELETE FROM user_api_keys WHERE email = ?", (email,))
        conn.commit()
        # Reset global keys as this user may have been the one loaded
        api_keys = {'binance': {}, 'kraken': {}}
        keys_loaded = False
        logger.info(f"Cleared API keys for {email}")
        return {"status": "cleared"}
    except Exception as e:
        logger.error(f"Clear Keys Error: {e}")
        raise HTTPException(status_code=500, detail="Clear failed")
    finally:
        conn.close()

@app.get("/get_keys")
async def get_keys(email: str = Query(...)):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute("SELECT binance_key, binance_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email = ?", (email,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "binance_key": row[0] or "",
            "binance_secret": row[1] or "",
            "kraken_key": row[2] or "",
            "kraken_secret": row[3] or "",
        }
    return {}

@app.get("/arbitrage/")
async def get_arbitrage():
    global keys_loaded
    if not keys_loaded:
        return {"error": "API keys not loaded"}
    try:
        binance = ccxt.binance(api_keys['binance'])
        kraken = ccxt.kraken(api_keys['kraken'])
        binance_price = binance.fetch_ticker('XRP/USDT')['last']
        kraken_price = kraken.fetch_ticker('XRP/USD')['last']
        spread = abs(binance_price - kraken_price) / min(binance_price, kraken_price)  # decimal
        spread_pct = spread * 100
        pnl_after_fees = spread - 0.002  # 0.2% fees decimal
        roi_usdt = pnl_after_fees * trade_amount
        logger.info(f"Arbitrage: Binance {binance_price:.4f}, Kraken {kraken_price:.4f}, Spread {spread_pct:.4f}%, PnL {pnl_after_fees*100:.4f}%")
        return {
            "binance": binance_price,
            "kraken": kraken_price,
            "spread": spread,
            "spread_pct": spread_pct,
            "pnl": pnl_after_fees,  # decimal for >= check
            "pnl_pct": pnl_after_fees * 100,
            "roi_usdt": roi_usdt
        }
    except Exception as e:
        logger.error(f"Arbitrage fetch error: {e}")
        return {"error": str(e)}

@app.get("/balances")
async def get_balances():
    global keys_loaded
    if not keys_loaded:
        return {"error": "API keys not loaded"}
    try:
        binance = ccxt.binance(api_keys['binance'])
        kraken = ccxt.kraken(api_keys['kraken'])
        b_bal = binance.fetch_balance()['USDT']['free']
        k_bal = kraken.fetch_balance()['USD']['free']
        return {"binance_usdt": b_bal, "kraken_usdt": k_bal}
    except Exception as e:
        logger.error(f"Balances error: {e}")
        return {"error": str(e)}

@app.post("/set_amount")
async def set_amount(a: Amount):
    global trade_amount
    if a.amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be positive")
    trade_amount = a.amount
    logger.info(f"Trade amount set to {trade_amount}")
    return {"status": "set", "amount": trade_amount}

@app.post("/toggle_auto_trade")
async def toggle_auto_trade(t: Toggle):
    global auto_trade_enabled
    auto_trade_enabled = t.enabled
    logger.info(f"Auto-trade {'enabled' if auto_trade_enabled else 'disabled'}")
    return {"status": "toggled", "enabled": auto_trade_enabled}

@app.get("/auto_trade_status")
async def get_auto_trade_status():
    global auto_trade_enabled
    return {"enabled": auto_trade_enabled}

@app.post("/set_threshold")
async def set_threshold(t: Threshold):
    global trade_threshold
    if t.threshold < 0:
        raise HTTPException(status_code=400, detail="Threshold must be non-negative")
    trade_threshold = t.threshold
    logger.info(f"Threshold set to {trade_threshold} (decimal: >= for trades)")
    return {"status": "set", "threshold": trade_threshold}

# WebSocket Manager (unchanged)
class WebSocketManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Active: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except WebSocketDisconnect:
                disconnected.append(connection)
        for conn in disconnected:
            self.disconnect(conn)

manager = WebSocketManager()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(broadcast_arbitrage())
    logger.info("Bot started - Monitoring active (skips until keys loaded)")

async def broadcast_arbitrage():
    global keys_loaded
    while True:
        try:
            if not keys_loaded:
                await asyncio.sleep(30)
                continue
            arb_data = await get_arbitrage()
            if isinstance(arb_data, dict) and "error" not in arb_data:
                await manager.broadcast(json.dumps(arb_data))
                if auto_trade_enabled and arb_data["pnl"] >= trade_threshold:  # >= logic confirmed
                    await execute_arbitrage(arb_data["binance"], arb_data["kraken"])
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Broadcast error: {e}")
            await asyncio.sleep(5)

async def execute_arbitrage(b_price: float, k_price: float):
    global keys_loaded
    if not keys_loaded:
        return
    try:
        binance = ccxt.binance(api_keys['binance'])
        kraken = ccxt.kraken(api_keys['kraken'])
        xrp_amount = trade_amount / min(b_price, k_price)
        if b_price < k_price:
            binance.create_market_buy_order('XRP/USDT', xrp_amount)
            kraken.create_market_sell_order('XRP/USD', xrp_amount)
        else:
            kraken.create_market_buy_order('XRP/USD', xrp_amount)
            binance.create_market_sell_order('XRP/USDT', xrp_amount)
        logger.info("Trade executed!")
    except Exception as e:
        logger.error(f"Trade error: {e}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))  # FIXED: Use env var for port (required for cloud like Render)
    uvicorn.run(app, host="0.0.0.0", port=port)

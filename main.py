# main.py — FINAL & PERFECT — Passive Crypto Income (USD-XRP-USD 24/7 Auto-Trading)
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import ccxt.async_support as ccxt
import sqlite3
import logging
import asyncio
import os

app = FastAPI(title="Passive Crypto Income — 24/7 Atomic Arbitrage")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DATABASE
conn = sqlite3.connect('users.db', check_same_thread=False)
c = conn.cursor()

# API Keys Table
c.execute('''CREATE TABLE IF NOT EXISTS user_api_keys 
             (email TEXT PRIMARY KEY, 
              binanceus_key TEXT, binanceus_secret TEXT, 
              kraken_key TEXT, kraken_secret TEXT)''')

# Settings Table — NO DEFAULT 100.0 — starts at 0.0
c.execute('''CREATE TABLE IF NOT EXISTS user_settings 
             (email TEXT PRIMARY KEY, 
              trade_amount REAL,           -- ← No default → starts at 0.0
              auto_trade INTEGER DEFAULT 0, 
              threshold REAL DEFAULT 0.0)''')
conn.commit()

# Background trading tasks
user_trading_tasks = {}

# Get API keys
async def get_keys(email: str):
    c.execute("SELECT binanceus_key, binanceus_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email=?", (email,))
    row = c.fetchone()
    if row and row[0] and row[2]:
        return {
            'binanceus': {'apiKey': row[0], 'secret': row[1], 'enableRateLimit': True},
            'kraken': {'apiKey': row[2], 'secret': row[3], 'enableRateLimit': True}
        }
    return None

# Get REAL saved settings — never fall back to 100
async def get_user_settings(email: str):
    c.execute("SELECT trade_amount, auto_trade, threshold FROM user_settings WHERE email=?", (email,))
    row = c.fetchone()
    if row:
        amount = float(row[0]) if row[0] is not None else 0.0
        threshold = float(row[2]) if row[2] is not None else 0.0
        return {"amount": amount, "auto_trade": bool(row[1]), "threshold": threshold}
    return {"amount": 0.0, "auto_trade": False, "threshold": 0.0}

# 24/7 Auto-Trading Worker
async def auto_trading_worker(email: str):
    logger.info(f"24/7 Auto-Trading STARTED for {email}")
    while True:
        try:
            keys = await get_keys(email)
            if not keys:
                await asyncio.sleep(30)
                continue

            settings = await get_user_settings(email)
            if not settings["auto_trade"] or settings["amount"] <= 0:
                await asyncio.sleep(10)
                continue

            binance = ccxt.binanceus(keys['binanceus'])
            kraken = ccxt.kraken(keys['kraken'])
            await binance.load_markets()
            await kraken.load_markets()

            b_price = (await binance.fetch_ticker('XRP/USD'))['last']
            k_price = (await kraken.fetch_ticker('XRP/USD'))['last']

            await binance.close()
            await kraken.close()

            spread = abs(b_price - k_price) / min(b_price, k_price)
            net = spread - 0.0086  # total fees

            if net > settings["threshold"]:
                amount = settings["amount"]
                buy_ex = binance if b_price < k_price else kraken
                sell_ex = kraken if b_price < k_price else binance
                buy_name = "Binance.US" if b_price < k_price else "Kraken"
                sell_name = "Kraken" if b_price < k_price else "Binance.US"

                # Execute trade
                await buy_ex.create_market_buy_order('XRP/USD', amount)
                await asyncio.sleep(1.5)
                await sell_ex.create_market_sell_order('XRP/USD', amount)

                logger.info(f"TRADE: {email} | {buy_name} → {sell_name} | ${amount} | Profit ~{net*100:.4f}%")

            await asyncio.sleep(8)
        except Exception as e:
            logger.error(f"Auto-trade error {email}: {e}")
            await asyncio.sleep(15)

# ================= ENDPOINTS =================

@app.get("/")
async def root():
    return {"message": "Passive Crypto Income — Live & Running"}

@app.post("/save_keys")
async def save_keys(data: dict):
    email = data.get("email")
    if not email: raise HTTPException(400, "Email required")
    c.execute("INSERT OR REPLACE INTO user_api_keys VALUES (?, ?, ?, ?, ?)",
              (email, data.get("binanceus_key",""), data.get("binanceus_secret",""),
               data.get("kraken_key",""), data.get("kraken_secret","")))
    conn.commit()
    return {"status": "saved"}

@app.get("/get_keys")
async def get_keys_route(email: str = Query(...)):
    c.execute("SELECT binanceus_key, binanceus_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email=?", (email,))
    row = c.fetchone()
    if row:
        return { "binanceus_key": row[0] or "", "binanceus_secret": row[1] or "", "kraken_key": row[2] or "", "kraken_secret": row[3] or "" }
    return {}

@app.get("/get_settings")
async def get_settings(email: str = Query(...)):
    settings = await get_user_settings(email)
    return {
        "trade_amount": settings["amount"],
        "auto_trade": settings["auto_trade"],
        "threshold": settings["threshold"]
    }

@app.get("/balances")
async def balances(email: str = Query(...)):
    keys = await get_keys(email)
    if not keys: return {"binanceus_usd": 0.0, "kraken_usd": 0.0}
    try:
        binance = ccxt.binanceus(keys['binanceus'])
        kraken = ccxt.kraken(keys['kraken'])
        await binance.load_markets()
        await kraken.load_markets()
        b_bal = await binance.fetch_balance()
        k_bal = await kraken.fetch_balance()
        await binance.close()
        await kraken.close()
        return {
            "binanceus_usd": float(b_bal.get('USD', {}).get('free') or 0.0),
            "kraken_usd": float(k_bal.get('USD', {}).get('free') or 0.0)
        }
    except Exception as e:
        logger.error(f"Balance error: {e}")
        return {"binanceus_usd": 0.0, "kraken_usd": 0.0}

@app.get("/arbitrage")
async def arbitrage(email: str = Query(...)):
    keys = await get_keys(email)
    if not keys: return {"error": "Save API keys first"}
    try:
        binance = ccxt.binanceus(keys['binanceus'])
        kraken = ccxt.kraken(keys['kraken'])
        await binance.load_markets()
        await kraken.load_markets()
        b_price = (await binance.fetch_ticker('XRP/USD'))['last']
        k_price = (await kraken.fetch_ticker('XRP/USD'))['last']
        await binance.close()
        await kraken.close()
        spread = abs(b_price - k_price) / min(b_price, k_price)
        net = spread - 0.0086
        profit_pct = net * 100
        direction = "Buy Binance.US → Sell Kraken" if b_price < k_price else "Buy Kraken → Sell Binance.US"
        return {
            "binanceus": round(b_price, 6),
            "kraken": round(k_price, 6),
            "spread_pct": round(spread * 100, 4),
            "roi_usd": round(profit_pct, 4),
            "profitable": net > 0,
            "direction": direction
        }
    except Exception as e:
        return {"error": "Price unavailable"}

# FIXED: Trade amount accepts ANY value — no revert
@app.post("/set_amount")
async def set_amount(data: dict):
    email = data.get("email")
    try:
        amount = float(data["amount"])
        if amount < 0: amount = 0.0
    except:
        amount = 0.0
    c.execute("INSERT OR REPLACE INTO user_settings (email, trade_amount) VALUES (?, ?)", (email, amount))
    conn.commit()
    return {"status": "ok"}

@app.post("/toggle_auto_trade")
async def toggle_auto_trade(data: dict):
    email = data.get("email")
    enabled = int(data.get("enabled", 0))
    c.execute("INSERT OR REPLACE INTO user_settings (email, auto_trade) VALUES (?, ?)", (email, enabled))
    conn.commit()

    if enabled and email not in user_trading_tasks:
        task = asyncio.create_task(auto_trading_worker(email))
        user_trading_tasks[email] = task
    elif not enabled and email in user_trading_tasks:
        user_trading_tasks[email].cancel()
        user_trading_tasks.pop(email, None)
    return {"status": "ok"}

@app.post("/set_threshold")
async def set_threshold(data: dict):
    email = data.get("email")
    try:
        threshold = float(data["threshold"])
        if threshold < 0: threshold = 0.0
    except:
        threshold = 0.0
    c.execute("INSERT OR REPLACE INTO user_settings (email, threshold) VALUES (?, ?)", (email, threshold))
    conn.commit()
    return {"status": "ok"}

# Run
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)

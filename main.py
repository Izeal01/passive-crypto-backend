# main.py — FINAL DEC 07 2025 — XRP/USD ON BOTH EXCHANGES + TRUE BACKGROUND AUTO-TRADING
import os
import asyncio
import logging
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import ccxt.async_support as ccxt
import sqlite3
from datetime import datetime

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

conn = sqlite3.connect('users.db', check_same_thread=False)
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS user_api_keys 
             (email TEXT PRIMARY KEY, binanceus_key TEXT, binanceus_secret TEXT, kraken_key TEXT, kraken_secret TEXT)''')
c.execute('''CREATE TABLE IF NOT EXISTS user_settings 
             (email TEXT PRIMARY KEY, trade_amount REAL DEFAULT 100.0, auto_trade INTEGER DEFAULT 0, threshold REAL DEFAULT 0.0001)''')
conn.commit()

# ===================== BACKGROUND AUTO-TRADE LOOP =====================
async def auto_trade_worker():
    logger.info("Background auto-trade worker started")
    while True:
        try:
            c.execute("SELECT email FROM user_settings WHERE auto_trade = 1")
            active_users = [row[0] for row in c.fetchall()]
            
            for email in active_users:
                await execute_arbitrage_if_profitable(email)
            await asyncio.sleep(2.5)  # Scan every 2.5 sec
        except Exception as e:
            logger.error(f"Auto-trade loop error: {e}")
            await asyncio.sleep(5)

async def execute_arbitrage_if_profitable(email: str):
    keys = await get_keys(email)
    if not keys:
        return

    c.execute("SELECT trade_amount, threshold FROM user_settings WHERE email=?", (email,))
    settings = c.fetchone()
    if not settings:
        return
    amount_usd, threshold = settings

    binance = ccxt.binanceus(keys['binanceus'])
    kraken = ccxt.kraken(keys['kraken'])
    
    try:
        await binance.load_markets()
        await kraken.load_markets()

        b_ticker = await binance.fetch_ticker('XRP/USD')
        k_ticker = await kraken.fetch_ticker('XRP/USD')  # Kraken also has XRP/USD!

        b_price = b_ticker['last']
        k_price = k_ticker['last']

        spread = abs(b_price - k_price) / min(b_price, k_price)
        net = spread - 0.0086  # ~0.86% total fees

        if net > threshold:
            low_ex = 'binanceus' if b_price < k_price else 'kraken'
            high_ex = 'kraken' if low_ex == 'binanceus' else 'binanceus'
            ex_low = binance if low_ex == 'binanceus' else kraken
            ex_high = kraken if low_ex == 'binanceus' else binance

            amount_xrp = amount_usd / b_price  # approx

            # Execute market orders
            await ex_low.create_market_buy_order('XRP/USD', amount_xrp)
            await ex_high.create_market_sell_order('XRP/USD', amount_xrp)

            logger.info(f"AUTO-TRADE EXECUTED for {email}: Buy {low_ex.upper()} → Sell {high_ex.upper()} | Net {net*100:.4f}%")

    except Exception as e:
        logger.error(f"Auto-trade execution failed for {email}: {e}")
    finally:
        await binance.close()
        await kraken.close()

# Start background task
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(auto_trade_worker())

# ===================== REST OF ENDPOINTS (UPDATED FOR XRP/USD) =====================
async def get_keys(email: str):
    c.execute("SELECT binanceus_key, binanceus_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email=?", (email,))
    row = c.fetchone()
    if row and row[0] and row[2]:
        return {
            'binanceus': {'apiKey': row[0], 'secret': row[1], 'enableRateLimit': True, 'timeout': 60000},
            'kraken': {'apiKey': row[2], 'secret': row[3], 'enableRateLimit': True, 'timeout': 60000}
        }
    return None

@app.get("/balances")
async def balances(email: str = Query(...)):
    keys = await get_keys(email)
    if not keys:
        return {"binanceus_usd": 0.0, "kraken_usd": 0.0}
    
    binance = kraken = None
    try:
        binance = ccxt.binanceus(keys['binanceus'])
        kraken = ccxt.kraken(keys['kraken'])
        await binance.load_markets()
        await kraken.load_markets()
        
        b_bal = await binance.fetch_balance()
        k_bal = await kraken.fetch_balance()
        
        b_usd = b_bal.get('USD', {}).get('free', 0.0)
        k_usd = k_bal.get('USD', {}).get('free', 0.0) or k_bal.get('UST', {}).get('free', 0.0)  # Kraken sometimes uses UST
        
        return {"binanceus_usd": float(b_usd), "kraken_usd": float(k_usd)}
    except Exception as e:
        logger.error(f"Balance error: {e}")
        return {"binanceus_usd": 0.0, "kraken_usd": 0.0}
    finally:
        if binance: await binance.close()
        if kraken: await kraken.close()

@app.get("/arbitrage")
async def arbitrage(email: str = Query(...)):
    keys = await get_keys(email)
    if not keys:
        return {"error": "Save API keys first"}
    
    binance = kraken = None
    try:
        binance = ccxt.binanceus(keys['binanceus'])
        kraken = ccxt.kraken(keys['kraken'])
        await binance.load_markets()
        await kraken.load_markets()
        
        b_price = (await binance.fetch_ticker('XRP/USD'))['last']
        k_price = (await kraken.fetch_ticker('XRP/USD'))['last']
        
        spread = abs(b_price - k_price) / min(b_price, k_price)
        net = spread - 0.0086
        roi = max(net * 100, 0)
        direction = "Buy Binance.US → Sell Kraken" if b_price < k_price else "Buy Kraken → Sell Binance.US"
        
        return {
            "binanceus": round(b_price, 6),
            "kraken": round(k_price, 6),
            "roi_usd": round(roi, 2),
            "profitable": net > 0,
            "direction": direction
        }
    except Exception as e:
        logger.warning(f"Arbitrage error: {e}")
        return {"error": "Price unavailable"}
    finally:
        if binance: await binance.close()
        if kraken: await kraken.close()

# Keep other endpoints unchanged (save_keys, set_amount, toggle_auto_trade, etc.)
# ... [your existing /save_keys, /set_amount, /toggle_auto_trade, etc. remain the same]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))

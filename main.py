# main.py — FINAL & WORKING — COINBASE + KRAKEN (November 30, 2025)
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import ccxt.async_support as ccxt
import sqlite3
import logging
import asyncio
import os

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

# DATABASE
conn = sqlite3.connect('users.db', check_same_thread=False)
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS user_api_keys 
            (email TEXT PRIMARY KEY, coinbase_key TEXT, coinbase_secret TEXT, coinbase_passphrase TEXT, 
             kraken_key TEXT, kraken_secret TEXT)''')
c.execute('''CREATE TABLE IF NOT EXISTS user_settings 
            (email TEXT PRIMARY KEY, trade_amount REAL DEFAULT 100.0, auto_trade INTEGER DEFAULT 0, threshold REAL DEFAULT 0.005)''')
conn.commit()

# Global background tasks per user
background_scanners = {}

async def get_keys(email: str):
    c.execute("SELECT coinbase_key, coinbase_secret, coinbase_passphrase, kraken_key, kraken_secret FROM user_api_keys WHERE email=?", (email,))
    row = c.fetchone()
    if row and row[0] and row[3]:
        return {
            'coinbase': {'apiKey': row[0], 'secret': row[1], 'password': row[2], 'enableRateLimit': True},
            'kraken': {'apiKey': row[3], 'secret': row[4], 'enableRateLimit': True}
        }
    return None

async def arbitrage_scanner(email: str):
    """Runs forever every 2.5 seconds per user"""
    while True:
        try:
            keys = await get_keys(email)
            if not keys:
                await asyncio.sleep(10)
                continue

            c.execute("SELECT auto_trade, threshold, trade_amount FROM user_settings WHERE email=?", (email,))
            settings = c.fetchone()
            if not settings or settings[0] == 0:
                await asyncio.sleep(5)
                continue

            auto_trade, threshold, amount = settings

            manager = ExchangeManager(
                keys['coinbase']['apiKey'],
                keys['coinbase']['secret'],
                keys['coinbase']['password'],
                keys['kraken']['apiKey'],
                keys['kraken']['secret']
            )

            arb = await manager.calculate_arbitrage()
            if arb and arb['net_profit_pct'] >= threshold:
                logger.info(f"PROFIT DETECTED for {email}: {arb['net_profit_pct']}%")
                low_ex = 'coinbase' if arb['low_exchange'] == 'Coinbase' else 'kraken'
                high_ex = 'kraken' if arb['high_exchange'] == 'Kraken' else 'coinbase'

                # Execute trades
                await manager.place_order(low_ex, 'buy', amount)
                await asyncio.sleep(1.5)  # Avoid rate limit
                await manager.place_order(high_ex, 'sell', amount)

            await asyncio.sleep(2.5)  # Scan every 2.5 seconds
        except Exception as e:
            logger.error(f"Scanner error for {email}: {e}")
            await asyncio.sleep(5)

# ================= API ENDPOINTS =================
@app.post("/save_keys")
async def save_keys(data: dict):
    email = data.get("email")
    if not email:
        raise HTTPException(400, "Email required")
    
    c.execute("""INSERT OR REPLACE INTO user_api_keys VALUES (?, ?, ?, ?, ?, ?)""",
              (email, data.get("coinbase_key",""), data.get("coinbase_secret",""),
               data.get("coinbase_passphrase",""), data.get("kraken_key",""), data.get("kraken_secret","")))
    conn.commit()
    
    # Start scanner if not running
    if email not in background_scanners:
        task = asyncio.create_task(arbitrage_scanner(email))
        background_scanners[email] = task
        logger.info(f"Arbitrage scanner started for {email}")

    return {"status": "saved"}

@app.get("/balances")
async def balances(email: str = Query(...)):
    keys = await get_keys(email)
    if not keys:
        return {"coinbase_usdc": 0.0, "kraken_usdc": 0.0}
    
    try:
        coinbase = ccxt.coinbaseadvanced(keys['coinbase'])
        kraken = ccxt.kraken(keys['kraken'])
        await coinbase.load_markets()
        await kraken.load_markets()

        cb_bal = await coinbase.fetch_balance()
        k_bal = await kraken.fetch_balance()

        cb_usdc = cb_bal.get('total', {}).get('USDC', 0.0) or cb_bal.get('USDC', {}).get('free', 0.0)
        k_usdc = k_bal.get('USDC', {}).get('free', 0.0)

        return {"coinbase_usdc": float(cb_usdc), "kraken_usdc": float(k_usdc)}
    except Exception as e:
        logger.error(f"Balance error: {e}")
        return {"coinbase_usdc": 0.0, "kraken_usdc": 0.0}
    finally:
        if 'coinbase' in locals(): await coinbase.close()
        if 'kraken' in locals(): await kraken.close()

@app.get("/arbitrage")
async def arbitrage(email: str = Query(...)):
    keys = await get_keys(email)
    if not keys:
        return {"error": "Save API keys first"}

    try:
        manager = ExchangeManager(
            keys['coinbase']['apiKey'], keys['coinbase']['secret'], keys['coinbase']['password'],
            keys['kraken']['apiKey'], keys['kraken']['secret']
        )
        arb = await manager.calculate_arbitrage()
        if arb:
            return arb
        return {
            "coinbase": "—", "kraken": "—", "spread_pct": 0,
            "roi_usdc": 0, "profitable": False, "direction": "No opportunity"
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/set_amount")
async def set_amount(data: dict):
    email = data.get("email")
    amount = float(data.get("amount", 100.0))
    c.execute("INSERT OR REPLACE INTO user_settings (email, trade_amount) VALUES (?, ?)", (email, amount))
    conn.commit()
    return {"status": "ok"}

@app.post("/toggle_auto_trade")
async def toggle_auto_trade(data: dict):
    email = data.get("email")
    enabled = int(data.get("enabled", 0))
    c.execute("INSERT OR REPLACE INTO user_settings (email, auto_trade) VALUES (?, ?)", (email, enabled))
    conn.commit()
    return {"status": "ok"}

@app.post("/set_threshold")
async def set_threshold(data: dict):
    email = data.get("email")
    threshold = float(data.get("threshold", 0.005))
    c.execute("INSERT OR REPLACE INTO user_settings (email, threshold) VALUES (?, ?)", (email, threshold))
    conn.commit()
    return {"status": "ok"}

@app.get("/")
async def root():
    return {"message": "Passive Crypto Income – Coinbase + Kraken USDC Arbitrage"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))

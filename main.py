# main.py — FINAL & COMPLETE — DEC 09 2025
import os
import asyncio
import logging
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import ccxt.async_support as ccxt
import sqlite3

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

# Database
conn = sqlite3.connect('users.db', check_same_thread=False)
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS user_api_keys 
             (email TEXT PRIMARY KEY, binanceus_key TEXT, binanceus_secret TEXT, kraken_key TEXT, kraken_secret TEXT)''')
c.execute('''CREATE TABLE IF NOT EXISTS user_settings 
             (email TEXT PRIMARY KEY, trade_amount REAL DEFAULT 0.0, auto_trade INTEGER DEFAULT 1, threshold REAL DEFAULT 0.0)''')
conn.commit()

async def get_keys(email: str):
    c.execute("SELECT binanceus_key, binanceus_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email=?", (email,))
    row = c.fetchone()
    if row and row[0] and row[2]:
        return {
            'binanceus': {'apiKey': row[0], 'secret': row[1], 'enableRateLimit': True},
            'kraken': {'apiKey': row[2], 'secret': row[3], 'enableRateLimit': True, 'options': {'adjustForTimeDifference': True}}
        }
    return None

# 24/7 AUTO-TRADER — ALWAYS RUNNING, EVEN WHEN APP IS CLOSED
async def auto_trade_worker():
    logger.info("24/7 AUTO-TRADER STARTED — TRADES EXECUTE EVEN WHEN APP IS CLOSED")
    while True:
        try:
            c.execute("SELECT email, trade_amount, threshold FROM user_settings WHERE auto_trade = 1 AND trade_amount > 0")
            users = c.fetchall()
            for email, amount_usd, threshold in users:
                keys = await get_keys(email)
                if not keys: continue

                binance = ccxt.binanceus(keys['binanceus'])
                kraken = ccxt.kraken(keys['kraken'])
                try:
                    await binance.load_markets()
                    await kraken.load_markets()
                    b_price = (await binance.fetch_ticker('XRP/USD'))['last']
                    k_price = (await kraken.fetch_ticker('XRP/USD'))['last']
                    spread = abs(b_price - k_price) / min(b_price, k_price)
                    net_profit_pct = (spread - 0.0086) * 100  # Can be negative

                    if net_profit_pct > threshold:
                        low_ex = binance if b_price < k_price else kraken
                        high_ex = kraken if b_price < k_price else binance
                        amount_xrp = amount_usd / ((b_price + k_price) / 2)
                        await low_ex.create_market_buy_order('XRP/USD', amount_xrp)
                        await high_ex.create_market_sell_order('XRP/USD', amount_xrp)
                        logger.info(f"TRADE EXECUTED | {email} | Net {net_profit_pct:.4f}% | ${amount_usd}")
                except Exception as e:
                    logger.error(f"Trade failed for {email}: {e}")
                finally:
                    await binance.close()
                    await kraken.close()
            await asyncio.sleep(6)
        except Exception as e:
            logger.error(f"Auto-trade loop error: {e}")
            await asyncio.sleep(10)

@app.on_event("startup")
async def startup():
    asyncio.create_task(auto_trade_worker())

# ENDPOINTS
@app.post("/login")
async def login():
    return {"status": "ok"}

@app.post("/save_keys")
async def save_keys(data: dict):
    email = data.get("email")
    if not email: raise HTTPException(400, "Email required")
    c.execute("INSERT OR REPLACE INTO user_api_keys VALUES (?, ?, ?, ?, ?)",
              (email, data.get("binanceus_key",""), data.get("binanceus_secret",""),
               data.get("kraken_key",""), data.get("kraken_secret","")))
    conn.commit()
    logger.info(f"Keys saved for {email}")
    return {"status": "saved"}

@app.get("/get_keys")
async def get_keys_route(email: str = Query(...)):
    c.execute("SELECT binanceus_key, binanceus_secret, kraken_key, kraken_secret FROM user_api_keys WHERE email=?", (email,))
    row = c.fetchone()
    if row:
        return {"binanceus_key": row[0] or "", "binanceus_secret": row[1] or "",
                "kraken_key": row[2] or "", "kraken_secret": row[3] or ""}
    return {}

@app.get("/balances")
async def balances(email: str = Query(...)):
    keys = await get_keys(email)
    if not keys: return {"binanceus_usd": 0.0, "kraken_usd": 0.0}
    binance = ccxt.binanceus(keys['binanceus'])
    kraken = ccxt.kraken(keys['kraken'])
    try:
        await binance.load_markets()
        await kraken.load_markets()
        b_bal = await binance.fetch_balance()
        k_bal = await kraken.fetch_balance()
        b_usd = b_bal.get('USD', {}).get('free', 0.0)
        k_usd = k_bal.get('USD', {}).get('free', 0.0) or k_bal.get('UST', {}).get('free', 0.0)
        return {"binanceus_usd": float(b_usd), "kraken_usd": float(k_usd)}
    except Exception as e:
        logger.error(f"Balance error: {e}")
        return {"binanceus_usd": 0.0, "kraken_usd": 0.0}
    finally:
        await binance.close()
        await kraken.close()

@app.get("/arbitrage")
async def arbitrage(email: str = Query(...)):
    keys = await get_keys(email)
    if not keys: return {"error": "Save API keys first"}
    binance = ccxt.binanceus(keys['binanceus'])
    kraken = ccxt.kraken(keys['kraken'])
    try:
        await binance.load_markets()
        await kraken.load_markets()
        b_price = (await binance.fetch_ticker('XRP/USD'))['last']
        k_price = (await kraken.fetch_ticker('XRP/USD'))['last']
        spread = abs(b_price - k_price) / min(b_price, k_price)
        net_profit_pct = (spread - 0.0086) * 100  # Real net profit (can be negative)
        direction = "Buy Binance.US to Sell Kraken" if b_price < k_price else "Buy Kraken to Sell Binance.US"
        return {
            "binanceus": round(b_price, 6),
            "kraken": round(k_price, 6),
            "roi_usd": round(net_profit_pct, 3),
            "profitable": net_profit_pct > 0,
            "direction": direction
        }
    except Exception as e:
        logger.warning(f"Arbitrage error: {e}")
        return {"error": "Price unavailable"}
    finally:
        await binance.close()
        await kraken.close()

@app.get("/get_settings")
async def get_settings(email: str = Query(...)):
    c.execute("SELECT auto_trade, trade_amount, threshold FROM user_settings WHERE email=?", (email,))
    row = c.fetchone()
    if row:
        return {"auto_trade": row[0], "trade_amount": row[1], "threshold": row[2]}
    return {"auto_trade": 1, "trade_amount": 0.0, "threshold": 0.0}

@app.post("/set_amount")
async def set_amount(data: dict):
    email = data.get("email")
    amount = max(float(data.get("amount", 0.0)), 0.0)
    c.execute("INSERT OR REPLACE INTO user_settings (email, trade_amount) VALUES (?, ?)", (email, amount))
    conn.commit()
    return {"status": "ok"}

@app.post("/toggle_auto_trade")
async def toggle_auto_trade(data: dict):
    email = data.get("email")
    enabled = int(data.get("enabled", 1))
    c.execute("INSERT OR REPLACE INTO user_settings (email, auto_trade) VALUES (?, ?)", (email, enabled))
    conn.commit()
    logger.info(f"Auto-trade {'ENABLED' if enabled else 'DISABLED'} for {email}")
    return {"status": "ok"}

@app.post("/set_threshold")
async def set_threshold(data: dict):
    email = data.get("email")
    threshold = max(float(data.get("threshold", 0.0)), 0.0)
    c.execute("INSERT OR REPLACE INTO user_settings (email, threshold) VALUES (?, ?)", (email, threshold))
    conn.commit()
    return {"status": "ok"}

@app.get("/")
async def root():
    return {"message": "Passive Crypto Income — Running 24/7 — Auto-trade ACTIVE"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))

import ccxt
import os
from dotenv import load_dotenv

load_dotenv()

class ExchangeManager:
    def __init__(self):
        self.binance = ccxt.binance({
            'apiKey': os.getenv('BINANCE_API_KEY'),
            'secret': os.getenv('BINANCE_SECRET'),
            'sandbox': True,  # Use testnet first!
        })
        self.kraken = ccxt.kraken({
            'apiKey': os.getenv('KRAKEN_API_KEY'),
            'secret': os.getenv('KRAKEN_SECRET'),
            # sandbox: True removed - Kraken does not support sandbox mode in CCXT
        })

    def get_price(self, exchange, symbol='XRP/USDT'):
        try:
            # Load markets once per exchange to avoid repeated asset calls
            if not exchange.markets:
                exchange.load_markets()
            ticker = exchange.fetch_ticker(symbol)
            return {
                'bid': ticker['bid'],
                'ask': ticker['ask'],
                'spread': (ticker['ask'] - ticker['bid']) / ticker['bid'] * 100
            }
        except Exception as e:
            print(f"Error fetching {exchange.id} for {symbol}: {e}")
            return None

    def calculate_arbitrage(self):
        # Normalize symbols for each exchange
        bin_symbol = 'XRPUSDT'  # Binance format
        kra_symbol = 'XRP/USD'  # Kraken format

        bin_price = self.get_price(self.binance, bin_symbol)
        kra_price = self.get_price(self.kraken, kra_symbol)
        if not bin_price or not kra_price:
            return None

        # Assume buy on low ask, sell on high bid
        low_ask = min(bin_price['ask'], kra_price['ask'])
        high_bid = max(bin_price['bid'], kra_price['bid'])
        gross_profit_pct = (high_bid - low_ask) / low_ask * 100

        # Estimate fees: 0.1% trading each side + 0.25 XRP withdraw (~$0.13 at $0.5/XRP)
        trading_fees = 0.002 * low_ask  # 0.1% buy + 0.1% sell
        withdraw_fee = 0.25 * 0.5  # Conservative
        net_profit = gross_profit_pct - (trading_fees + withdraw_fee) / low_ask * 100

        if net_profit > 0.5:  # Threshold
            low_ex = 'Binance' if bin_price['ask'] == low_ask else 'Kraken'
            high_ex = 'Kraken' if kra_price['bid'] == high_bid else 'Binance'
            return {
                'low_exchange': low_ex,
                'high_exchange': high_ex,
                'low_price': low_ask,
                'high_price': high_bid,
                'gross_profit': gross_profit_pct,
                'net_profit': net_profit,
                'timestamp': 'now'
            }
        return None

    def place_order(self, exchange_name, side, amount, price=None):
        ex = self.binance if exchange_name == 'Binance' else self.kraken
        # Normalize symbol for exchange
        symbol = 'XRPUSDT' if exchange_name == 'Binance' else 'XRP/USD'
        try:
            if side == 'buy':
                order = ex.create_market_buy_order(symbol, amount)
            else:
                order = ex.create_market_sell_order(symbol, amount)
            return order
        except Exception as e:
            return {'error': str(e)}

import logging
import time
import numpy as np
import pandas as pd
from datetime import datetime
from binance import ThreadedWebsocketManager
from binance.client import Client
import config
import argparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


class VolumeAnalysis:
    def __init__(self):
        # Matches:
        # var float cnv_daily = 0.0
        # var float cnv_dailyr = 0.0
        # var int last_day = na
        self.cnv_daily = 0.0
        self.cnv_dailyr = 0.0
        self.last_day = None

        # Matches: Length = input(2)
        self.length = 2
        self.prev_close = None
        self.values = []

    def calculate_change(self, current_close):
        # Matches: ta.change(srcc)
        if self.prev_close is None:
            return 0
        return current_close - self.prev_close

    def process_data(self, timestamp, close, open_price, volume):
        # Matches:
        # cnv_daily := last_day != dayofweek(time) ? 0 : cnv_daily
        # cnv_dailyr := last_day != dayofweek(time) ? 0 : cnv_dailyr
        # last_day := dayofweek(time)
        current_day = pd.Timestamp(timestamp).dayofweek
        if self.last_day != current_day:
            self.cnv_daily = 0
            self.cnv_dailyr = 0
        self.last_day = current_day

        # Matches: srcc = close
        srcc = close

        # Matches: change_1 = ta.change(srcc)
        change_1 = self.calculate_change(srcc)

        # Matches:
        # nv = ta.change(srcc) > 0 ? math.abs(volume * close) : change_1 < 0 ? -volume * math.abs(close) : 0 * volume
        if change_1 > 0:
            nv = abs(volume * close)
        elif change_1 < 0:
            nv = -volume * abs(close)
        else:
            nv = 0 * volume

        # Matches: aa = (close - close[1]) / (close)
        aa = (close - (self.prev_close if self.prev_close is not None else close)) / close

        # Matches: bb = (close - open) / (close)
        bb = (close - open_price) / close

        # Matches: nvol = math.sign(ta.change(close)) * volume * math.abs(close)
        nvol = np.sign(change_1) * volume * abs(close)

        # Store current close for next iteration
        self.prev_close = close

        return {
            'timestamp': timestamp,
            'cnv_daily': self.cnv_daily,
            'cnv_dailyr': self.cnv_dailyr,
            'current_day': current_day,
            'change_1': change_1,
            'nv': nv,
            'aa': aa,
            'bb': bb,
            'nvol': nvol
        }


class BinanceDataStream:  # Changed class name to match your error
    def __init__(self, symbol="TROYUSDT"):
        logging.info(f"Initializing BinanceStream for {symbol}...")
        self.client = Client(config.API_KEY, config.API_SECRET)
        self.symbol = symbol.lower()
        self.twm = ThreadedWebsocketManager(api_key=config.API_KEY, api_secret=config.API_SECRET)
        self.volume_analyzer = VolumeAnalysis()

        self.current_bar = {
            'timestamp': None,
            'open': None,
            'high': None,
            'low': None,
            'close': None,
            'volume': 0.0
        }

    def process_message(self, msg):
        try:
            timestamp = datetime.fromtimestamp(msg['T'] / 1000)
            price = float(msg['p'])
            quantity = float(msg['q'])

            current_time = int(msg['T'] / 1000)
            bar_start_time = (current_time // 10) * 10

            if self.current_bar['timestamp'] is None:
                self.start_new_bar(bar_start_time, price)
            elif bar_start_time > self.current_bar['timestamp']:
                self.close_current_bar()
                self.start_new_bar(bar_start_time, price)

            self.update_bar(price, quantity)

        except Exception as e:
            logging.error(f"Error processing message: {e}")

    def start_new_bar(self, timestamp, price):
        self.current_bar = {
            'timestamp': timestamp,
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': 0.0
        }
        logging.debug(f"New bar started at {timestamp}")

    def update_bar(self, price, quantity):
        self.current_bar['high'] = max(self.current_bar['high'], price)
        self.current_bar['low'] = min(self.current_bar['low'], price)
        self.current_bar['close'] = price
        self.current_bar['volume'] += quantity

    def close_current_bar(self):
        if self.current_bar['timestamp'] is not None:
            timestamp = datetime.fromtimestamp(self.current_bar['timestamp'])

            result = self.volume_analyzer.process_data(
                timestamp=timestamp,
                close=self.current_bar['close'],
                open_price=self.current_bar['open'],
                volume=self.current_bar['volume']
            )

            logging.info(f"""
=== Bar Completed at {timestamp} ===
Symbol: {self.symbol.upper()}
Price: {self.current_bar['close']}
Volume: {self.current_bar['volume']}
CNV Daily: {result['cnv_daily']:.8f}
CNV Daily R: {result['cnv_dailyr']:.8f}
Change: {result['change_1']:.8f}
NV: {result['nv']:.8f}
AA: {result['aa']:.8f}
BB: {result['bb']:.8f}
NVOL: {result['nvol']:.8f}
Day of Week: {result['current_day']}
============================
            """)

    def start(self):  # Added start method
        logging.info("Starting WebSocket stream...")
        self.twm.start()
        self.twm.start_trade_socket(
            symbol=self.symbol,
            callback=self.process_message
        )
        logging.info("WebSocket stream started successfully")


def main():
    parser = argparse.ArgumentParser(description='Binance trading pair analysis')
    parser.add_argument('--symbol', type=str, default='TROYUSDT',
                        help='Trading pair symbol (default: TROYUSDT)')

    args = parser.parse_args()
    symbol = args.symbol.upper()

    logging.info(f"Starting analysis for {symbol}")

    try:
        stream = BinanceDataStream(symbol)  # Updated class name
        stream.start()  # Updated method name

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
        stream.twm.stop()
    except Exception as e:
        logging.error(f"Error in main loop: {e}")
        if 'stream' in locals():
            stream.twm.stop()


if __name__ == "__main__":
    main()
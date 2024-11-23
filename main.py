import logging
import time
import numpy as np
import pandas as pd
from datetime import datetime
from binance import ThreadedWebsocketManager
from binance.client import Client
import config
import argparse
from collections import deque


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


class VolumeAnalysis:
    def __init__(self):
        # Initial variables exactly as in Pine Script
        self.cnv_daily = 0.0  # var float cnv_daily = 0.0
        self.cnv_dailyr = 0.0  # var float cnv_dailyr = 0.0
        self.last_day = None  # var int last_day = na
        self.length = 2  # Length = input(2)
        self.prev_close = None  # for close[1]
        self.nvol_non_dailyy = 0.0  # var float nvol_non_dailyy = 0.0

        # For storing daily nvol values
        self.daily_nvol = 0.0
        self.daily_nvol_history = deque(maxlen=30)  # Store last 30 days of daily nvol
        self.nvol_non_dailyyys = 0.0  # To store daily security value

    def calculate_change(self, current_close):
        """Calculate price change (equivalent to ta.change)"""
        if self.prev_close is None:
            return 0
        return current_close - self.prev_close

    def process_data(self, timestamp, close, open_price, volume):
        current_day = pd.Timestamp(timestamp).dayofweek

        # Reset values on new day
        if self.last_day != current_day:
            self.cnv_daily = 0  # cnv_daily := last_day != dayofweek(time) ? 0 : cnv_daily
            self.cnv_dailyr = 0  # cnv_dailyr := last_day != dayofweek(time) ? 0 : cnv_dailyr
            self.nvol_non_dailyy = 0  # nvol_non_dailyy := last_day != dayofweek(time) ? 0 : nvol_non_dailyy

            # Store previous day's nvol in history
            if self.daily_nvol != 0:
                self.daily_nvol_history.append(self.daily_nvol)
            self.daily_nvol = 0

        self.last_day = current_day  # last_day := dayofweek(time)

        srcc = close  # srcc = close
        change_1 = self.calculate_change(srcc)  # change_1 = ta.change(srcc)

        # nv calculation
        if change_1 > 0:
            nv = abs(volume * close)
        elif change_1 < 0:
            nv = -volume * abs(close)
        else:
            nv = 0 * volume

        aa = (close - (self.prev_close if self.prev_close is not None else close)) / close
        bb = (close - open_price) / close

        nvol = np.sign(change_1) * volume * abs(close)

        # Update daily nvol
        self.daily_nvol += nvol  # Accumulate nvol for the day
        nvol_non_daily = self.daily_nvol  # request.security(syminfo.tickerid, "D", nvol)

        # Calculate nvol_non_dailyyy exactly as in Pine Script
        nvol_non_dailyyy = 0
        history_len = len(self.daily_nvol_history)
        if history_len > 0:
            total = sum(abs(x) for x in self.daily_nvol_history)
            nvol_non_dailyyy = total / 30

        # Calculate nvol_non_dailyyys (daily security value)
        if self.last_day != current_day:
            self.nvol_non_dailyyys = nvol_non_dailyyy  # Update daily security value on day change

        # Update daily values
        self.cnv_daily += nv  # cnv_daily := cnv_daily + nv
        self.cnv_dailyr += nv  # cnv_dailyr := cnv_dailyr + nv
        self.nvol_non_dailyy += nvol_non_dailyyy  # nvol_non_dailyy := nvol_non_dailyy + nvol_non_dailyyy

        self.prev_close = close

        return {
            'timestamp': timestamp,
            'cnv_daily': self.cnv_daily,
            'cnv_dailyr': self.cnv_dailyr,
            'change_1': change_1,
            'nv': nv,
            'aa': aa,
            'bb': bb,
            'nvol': nvol,
            'nvol_non_daily': nvol_non_daily,
            'nvol_non_dailyy': self.nvol_non_dailyy,
            'nvol_non_dailyyy': nvol_non_dailyyy,
            'nvol_non_dailyyys': self.nvol_non_dailyyys,
            'current_day': current_day
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
    Price: {self.current_bar['close']:.8f}
    Volume: {self.current_bar['volume']:.8f}
    CNV Daily: {result['cnv_daily']:.8f}
    CNV Daily R: {result['cnv_dailyr']:.8f}
    Change: {result['change_1']:.8f}
    NV: {result['nv']:.8f}
    AA: {result['aa']:.8f}
    BB: {result['bb']:.8f}
    NVOL: {result['nvol']:.8f}
    NVOL Non Daily: {result['nvol_non_daily']:.8f}
    NVOL Non Daily Y: {result['nvol_non_dailyy']:.8f}
    NVOL Non Daily YYY: {result['nvol_non_dailyyy']:.8f}
    NVOL Non Daily YYYS: {result['nvol_non_dailyyys']:.8f}
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
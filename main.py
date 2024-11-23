import logging
from binance.client import Client
from binance.exceptions import BinanceAPIException
from binance.streams import BinanceSocketManager
import pandas as pd
import numpy as np
import sys
import config
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import asyncio


class CryptoAnalyzer:
    def __init__(self, api_key, api_secret, testnet=True):
        try:
            self.client = Client(api_key, api_secret, testnet=testnet)
            self.data = []
            self.df = pd.DataFrame()
            self.running = True

            # Create a maximized window
            plt.style.use('dark_background')  # Optional: use dark theme
            self.fig = plt.figure(figsize=(16, 9))  # 16:9 aspect ratio
            manager = plt.get_current_fig_manager()

            # Different commands for different backends
            try:
                # Qt backend
                manager.window.showMaximized()
            except:
                try:
                    # TkAgg backend
                    manager.window.state('zoomed')  # Windows
                except:
                    try:
                        # WX backend
                        manager.frame.Maximize(True)
                    except:
                        try:
                            # GTK backend
                            manager.window.maximize()
                        except:
                            print("Could not maximize window")

            # Create subplots with adjusted heights
            gs = self.fig.add_gridspec(3, 1, height_ratios=[2, 1, 1], hspace=0.3)
            self.ax1 = self.fig.add_subplot(gs[0])
            self.ax2 = self.fig.add_subplot(gs[1])
            self.ax3 = self.fig.add_subplot(gs[2])

            # Set up the figure
            self.fig.canvas.mpl_connect('close_event', self.on_close)
            plt.tight_layout()

            logging.info("Binance client initialized successfully")
        except Exception as e:
            logging.error(f"Failed to initialize Binance client: {e}")
            exit(1)

    def on_close(self, event):
        self.running = False

    async def process_message(self, msg):
        try:
            if 'k' in msg:
                kline = msg['k']
                timestamp = pd.to_datetime(kline['t'], unit='ms')

                new_data = {
                    'timestamp': timestamp,
                    'open': float(kline['o']),
                    'high': float(kline['h']),
                    'low': float(kline['l']),
                    'close': float(kline['c']),
                    'volume': float(kline['v'])
                }

                if self.df.empty:
                    self.df = pd.DataFrame([new_data])
                else:
                    self.df = pd.concat([self.df, pd.DataFrame([new_data])], ignore_index=True)

                self.df = self.df.tail(100)
                self.calculate_metrics()
                self.update_plot()

        except Exception as e:
            logging.error(f"Error processing message: {e}")

    def calculate_metrics(self):
        try:
            # Get current day of week
            self.df['dayofweek'] = self.df['timestamp'].dt.dayofweek

            # Initialize daily variables if they don't exist
            if 'cnv_daily' not in self.df.columns:
                self.df['cnv_daily'] = 0.0
            if 'cnv_dailyr' not in self.df.columns:
                self.df['cnv_dailyr'] = 0.0

            # Reset values on new day
            day_changed = self.df['dayofweek'] != self.df['dayofweek'].shift(1)
            self.df.loc[day_changed, 'cnv_daily'] = 0
            self.df.loc[day_changed, 'cnv_dailyr'] = 0

            # Basic calculations
            self.df['price_change'] = self.df['close'].diff()
            self.df['close_prev'] = self.df['close'].shift(1)
            self.df['aa'] = (self.df['close'] - self.df['close_prev']) / self.df['close'].replace(0, np.nan)
            self.df['bb'] = (self.df['close'] - self.df['open']) / self.df['close'].replace(0, np.nan)

            # Calculate nv (net volume)
            self.df['nv'] = np.where(
                self.df['price_change'] > 0,
                abs(self.df['volume'] * self.df['close']),
                np.where(
                    self.df['price_change'] < 0,
                    -self.df['volume'] * abs(self.df['close']),
                    0
                )
            )

            # Calculate nvol
            self.df['nvol'] = np.sign(self.df['price_change']) * self.df['volume'] * abs(self.df['close'])

            # Calculate daily nvol
            self.df['nvol_non_daily'] = self.df.groupby(self.df['timestamp'].dt.date)['nvol'].transform('sum')

            # Calculate 30-day moving average of absolute nvol
            self.df['nvol_non_dailyyy'] = self.df['nvol_non_daily'].abs().rolling(window=30, min_periods=1).mean()

            # Calculate scaled versions
            self.df['nvol_non_dailyyy1'] = self.df['nvol_non_dailyyy'] / 576

            # Calculate nvol1
            self.df['nvol1'] = (self.df['bb'].fillna(0) * self.df['nvol_non_dailyyy1'] * 700) + self.df['nv']

            # Cumulative calculations
            self.df['cnv_daily'] = self.df.groupby(self.df['dayofweek'])['nv'].cumsum()
            self.df['cnv_dailyr'] = self.df.groupby(self.df['dayofweek'])['nv'].cumsum()

            # Calculate SMAs
            Length = 2
            self.df['cnv_tb'] = self.df['cnv_daily'] - self.df['cnv_daily'].rolling(window=Length).mean()
            self.df['cnv_tbb'] = self.df['cnv_dailyr'] - self.df['cnv_dailyr'].rolling(window=Length).mean()

            # Calculate nvv
            self.df['nvv'] = np.where(
                self.df['price_change'] > 0,
                abs(self.df['volume'] * self.df['close']),
                self.df['volume'] * abs(self.df['close'])
            )

            # Calculate additional metrics
            self.df['cnv_dailyy'] = self.df.groupby(self.df['dayofweek'])['nvv'].cumsum()
            self.df['cnv_tbbb'] = self.df['cnv_dailyy'] - self.df['cnv_dailyy'].rolling(window=Length).mean()
            self.df['rcnv_dailyy'] = self.df['cnv_daily'] / self.df['cnv_dailyy'].replace(0, np.nan)

            # Calculate nv1
            self.df['nv1'] = ((self.df['close'] - self.df['close_prev']) / self.df['close'].replace(0, np.nan)) * 100

            # Final calculations
            self.df['cnv_dailyyy1'] = self.df.groupby(self.df['dayofweek'])['nv1'].cumsum()
            self.df['cnv_tbbb1'] = self.df['cnv_dailyyy1'] - self.df['cnv_dailyyy1'].rolling(window=Length).mean()

            # Calculate nvv1
            self.df['nvv1'] = np.where(
                self.df['cnv_dailyyy1'] > 0,
                abs(self.df['volume'] * self.df['close']),
                np.where(
                    self.df['cnv_dailyyy1'] < 0,
                    -self.df['volume'] * abs(self.df['close']),
                    (self.df['volume'] * abs(self.df['close']) / 2)
                )
            )

            self.df['cnv_dailyyy2'] = self.df.groupby(self.df['dayofweek'])['nvv1'].cumsum()

            # Final ratio with safety check for division by zero
            self.df['rrcnv_daily'] = self.df['cnv_dailyy'] / self.df['nvol_non_dailyyy'].replace(0, np.nan)

            # Fill NaN values with 0
            self.df = self.df.fillna(0)

        except Exception as e:
            logging.error(f"Error calculating metrics: {str(e)}")

    def update_plot(self):
        try:
            if len(self.df) == 0:
                return

            self.ax1.clear()
            self.ax2.clear()
            self.ax3.clear()

            # Plot price and indicators
            self.ax1.plot(range(len(self.df)), self.df['close'], label='Price', color='blue')
            self.ax1.plot(range(len(self.df)), self.df['cnv_tb'], label='CNV TB', color='red', alpha=0.7)
            self.ax1.set_title('Price and CNV TB', fontsize=12, pad=20)
            self.ax1.grid(True)
            self.ax1.legend(fontsize=10)

            # Plot volumes
            self.ax2.bar(range(len(self.df)), self.df['volume'],
                         alpha=0.3, label='Volume', color='gray')
            self.ax2.plot(range(len(self.df)), self.df['nvol1'], label='NVOL1', color='green')
            self.ax2.set_title('Volume and NVOL1', fontsize=12, pad=20)
            self.ax2.grid(True)
            self.ax2.legend(fontsize=10)

            # Plot net volume indicators
            self.ax3.plot(range(len(self.df)), self.df['cnv_daily'], label='CNV Daily', color='blue')
            self.ax3.plot(range(len(self.df)), self.df['cnv_dailyr'], label='CNV DailyR', color='red')
            self.ax3.plot(range(len(self.df)), self.df['rrcnv_daily'], label='RRCNV Daily', color='green')
            self.ax3.set_title('Net Volume Indicators', fontsize=12, pad=20)
            self.ax3.grid(True)
            self.ax3.legend(fontsize=10)

            # Format axes and update
            for ax in [self.ax1, self.ax2, self.ax3]:
                ax.tick_params(axis='x', rotation=45, labelsize=10)
                ax.tick_params(axis='y', labelsize=10)

            current_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            self.fig.suptitle(f'Live Crypto Analysis - {current_time}', fontsize=14, y=0.995)

            # Adjust layout while maintaining maximized window
            plt.tight_layout(rect=[0, 0.03, 1, 0.95])

            # Update display
            plt.pause(0.1)

        except Exception as e:
            logging.error(f"Error updating plot: {str(e)}")

    async def start_streaming(self, symbol):
        try:
            print(f"Starting stream for {symbol}...")

            # Get initial historical data
            klines = self.client.get_historical_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_1MINUTE,
                limit=100
            )

            # Initialize dataframe
            initial_data = []
            for k in klines:
                initial_data.append({
                    'timestamp': pd.to_datetime(k[0], unit='ms'),
                    'open': float(k[1]),
                    'high': float(k[2]),
                    'low': float(k[3]),
                    'close': float(k[4]),
                    'volume': float(k[5])
                })

            self.df = pd.DataFrame(initial_data)
            self.calculate_metrics()
            self.update_plot()

            # Start websocket
            bm = BinanceSocketManager(self.client)
            socket = bm.kline_socket(symbol=symbol, interval='1m')

            async with socket as sock:
                while self.running:
                    try:
                        msg = await asyncio.wait_for(sock.recv(), timeout=5.0)
                        await self.process_message(msg)
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        if self.running:
                            logging.error(f"Socket error: {e}")
                            await asyncio.sleep(5)
                        else:
                            break

        except Exception as e:
            logging.error(f"Error starting stream: {e}")
        finally:
            plt.close('all')


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Set the backend to TkAgg (or Qt5Agg if you prefer)
    import matplotlib
    matplotlib.use('TkAgg')  # or 'Qt5Agg'

    symbol = sys.argv[1].upper() if len(sys.argv) > 1 else 'TROYUSDT'
    print(f"Analyzing {symbol}")

    analyzer = CryptoAnalyzer(
        api_key=config.API_KEY,
        api_secret=config.API_SECRET,
        testnet=True
    )

    try:
        plt.ion()
        await analyzer.start_streaming(symbol)
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    finally:
        plt.close('all')


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram terminated by user")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        plt.close('all')
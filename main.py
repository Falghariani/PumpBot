import logging
from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd
import numpy as np
import config
import time
from typing import Optional, Dict, Any
import colorama
from colorama import Fore, Style
import asyncio
import argparse
import signal
import sys
from typing import NoReturn
import sys
import locale
import io

# Set console encoding to UTF-8
if sys.platform.startswith('win'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Initialize colorama for colored terminal output
colorama.init()


class CryptoVolumeAnalyzer:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = True):
        # ... existing init code ...
        self.data_cache = None
        self.last_cache_update = 0
        self.cache_validity = 10  # seconds

    async def get_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Get data with caching"""
        now = time.time()
        if (self.data_cache is not None and
                now - self.last_cache_update < self.cache_validity):
            return self.data_cache

        data = await self.fetch_historical_data(symbol)
        if data is not None:
            self.data_cache = data
            self.last_cache_update = now
        return data


class ConfigurationError(Exception):
    """Custom exception for configuration errors"""
    pass


def setup_logging() -> None:
    """Configure logging settings"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('crypto_analysis.log', encoding='utf-8')  # Add encoding here
        ]
    )


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Crypto Volume Analysis Tool')
    parser.add_argument('symbol', nargs='?', default='TROYUSDT',
                        help='Trading pair symbol (default: TROYUSDT)')
    parser.add_argument('--interval', type=int, default=10,
                        help='Update interval in seconds (default: 10)')
    parser.add_argument('--test', action='store_true',
                        help='Use testnet instead of live trading')
    return parser.parse_args()


def validate_config() -> None:
    """Validate configuration settings"""
    required_configs = ['API_KEY', 'API_SECRET']

    for config_item in required_configs:
        if not hasattr(config, config_item) or getattr(config, config_item) is None:
            raise ConfigurationError(f"Missing required configuration: {config_item}")


class CryptoAnalysisApp:
    def __init__(self):
        self.analyzer = None
        self.running = False

    def signal_handler(self, signum: int, frame) -> None:
        """Handle interrupt signals"""
        print("\nReceived interrupt signal. Shutting down gracefully...")
        self.running = False

    async def run(self, symbol: str, interval: int, use_testnet: bool) -> None:
        """
        Run the main application loop

        Args:
            symbol (str): Trading pair to monitor
            interval (int): Update interval in seconds
            use_testnet (bool): Whether to use testnet
        """
        try:
            # Initialize analyzer
            self.analyzer = CryptoVolumeAnalyzer(
                api_key=config.API_KEY,
                api_secret=config.API_SECRET,
                testnet=use_testnet
            )

            # Start monitoring
            self.running = True
            await self.analyzer.monitor_trading_pair(symbol, interval)

        except KeyboardInterrupt:
            print("\nShutting down by user request...")
        except Exception as e:
            logging.error(f"Application error: {e}")
            raise
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        """Perform cleanup operations"""
        self.running = False
        logging.info("Cleanup completed")


async def main() -> NoReturn:
    """Main program execution"""
    try:
        # Setup
        setup_logging()
        args = parse_arguments()
        validate_config()

        # Log startup information
        logging.info(f"Starting analysis for {args.symbol}")
        logging.info(f"Update interval: {args.interval} seconds")
        logging.info(f"Using {'testnet' if args.test else 'live'} trading")

        # Initialize and run application
        app = CryptoAnalysisApp()

        # Set up signal handlers
        signal.signal(signal.SIGINT, app.signal_handler)
        signal.signal(signal.SIGTERM, app.signal_handler)

        # Run the application
        await app.run(args.symbol, args.interval, args.test)

    except ConfigurationError as e:
        logging.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
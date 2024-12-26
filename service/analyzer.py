from pathlib import Path
import json
import queue
from datetime import datetime
import numpy as np
import pandas as pd

from service.App import App
from common.utils import *
from common.classifiers import *
from common.model_store import *
from common.generators import generate_feature_set, predict_feature_set
from scripts.merge import *
from scripts.features import *

import logging
log = logging.getLogger('analyzer')


class Analyzer:
    """
    Represents an in-memory database reflecting the current state of the trading environment, including historical data.
    """

    def __init__(self, config):
        """
        Initializes the Analyzer with configuration parameters defining the database schema and operational settings.

        :param config: Configuration dictionary for initializing the Analyzer.
        """
        self.config = config
        self.klines = {}
        self.queue = queue.Queue()

        symbol = App.config["symbol"]
        data_path = Path(App.config["data_folder"]) / symbol
        model_path = Path(App.config["model_folder"]).resolve() if Path(App.config["model_folder"]).is_absolute() else (data_path / App.config["model_folder"]).resolve()

        self.models = load_models(model_path, App.config["labels"], App.config["algorithms"])
        App.transaction = load_last_transaction()

    def get_klines_count(self, symbol):
        """ Returns the number of kline records stored for a given symbol. """
        return len(self.klines.get(symbol, []))

    def get_last_kline(self, symbol):
        """ Returns the last kline record for a given symbol. """
        return self.klines.get(symbol, [])[-1] if self.get_klines_count(symbol) > 0 else None

    def get_last_kline_ts(self, symbol):
        """ Returns the timestamp of the last kline for a given symbol. """
        last_kline = self.get_last_kline(symbol)
        return last_kline[0] if last_kline else 0

    def get_missing_klines_count(self, symbol):
        """ Calculates the number of klines missing since the last recorded kline to the current time. """
        last_kline_ts = self.get_last_kline_ts(symbol)
        if not last_kline_ts:
            return App.config["features_horizon"]

        freq = App.config["freq"]
        now = datetime.utcnow()
        last_kline = datetime.utcfromtimestamp(last_kline_ts // 1000)
        interval_length = pd.Timedelta(freq).to_pytimedelta()
        intervals_count = (now - last_kline) // interval_length
        return intervals_count + 2

    def store_klines(self, data: dict):
        """ Stores or updates klines data for specified symbols. """
        now_ts = now_timestamp()
        freq = App.config["freq"]
        interval_length_ms = pandas_interval_length_ms(freq)

        for symbol, klines in data.items():
            self.klines.setdefault(symbol, [])
            klines_data = self.klines[symbol]
            ts = klines[0][0]  # First timestamp in the new klines data
            existing_indexes = [i for i, x in enumerate(klines_data) if x[0] >= ts]

            if existing_indexes:
                start = min(existing_indexes)
                del klines_data[start:]

            klines_data.extend(klines)
            klines_data = self._maintain_kline_window(klines_data, interval_length_ms, symbol)

    def _maintain_kline_window(self, klines_data, interval_length_ms, symbol):
        """ Maintains a fixed window of kline data to ensure it doesn't grow indefinitely. """
        kline_window = App.config["features_horizon"]
        if len(klines_data) > kline_window:
            klines_data = klines_data[-kline_window:]
        
        # Verify the sequence and timing of klines
        prev_ts = None
        for i, kline in enumerate(klines_data):
            ts = kline[0]
            if i > 0 and (ts - prev_ts != interval_length_ms):
                log.error(f"Kline sequence error in {symbol}. Expected interval: {interval_length_ms}, found: {ts - prev_ts}")
            prev_ts = ts

        return klines_data

    def analyze(self):
        """ Analyze the current klines to generate features, apply models, and produce trading signals. """
        symbol = App.config["symbol"]
        log.info(f"Analyzing {symbol}. Last kline timestamp: {self.get_last_kline_ts(symbol)}")

        # Placeholder for the main analysis logic
        # This would include data merging, feature generation, model application, and signal generation

        # Example of logging the final actions
        log.info("Analysis complete. Signals generated.")

if __name__ == "__main__":
    config = {}  # Example configuration dictionary
    analyzer = Analyzer(config)
    analyzer.analyze()

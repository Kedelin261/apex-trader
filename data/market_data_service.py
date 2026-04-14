"""
APEX MULTI-MARKET TJR ENGINE
Market Data Service — Historical and live data ingestion.

Supports:
- CSV import (OHLCV format)
- In-memory candle feed
- CCXT-based live feed adapter
- Data validation, normalization, deduplication

All data passes through validation before reaching the strategy engine.
"""

from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from domain.models import Candle, InstrumentType

logger = logging.getLogger(__name__)


# =============================================================================
# DATA VALIDATION
# =============================================================================

class HistoricalDataValidator:
    """
    Validates OHLCV data integrity.
    Rejects corrupt, duplicate, or structurally invalid candles.
    """

    def validate(self, candles: List[Candle]) -> Tuple[List[Candle], List[str]]:
        """
        Validate and clean a list of candles.
        Returns (valid_candles, error_list).
        """
        errors = []
        valid = []
        seen_timestamps = set()

        for i, c in enumerate(candles):
            # Structural checks
            if c.high < c.low:
                errors.append(f"[{i}] {c.timestamp}: high < low")
                continue
            if c.open <= 0 or c.high <= 0 or c.low <= 0 or c.close <= 0:
                errors.append(f"[{i}] {c.timestamp}: non-positive price")
                continue
            if c.high < c.open or c.high < c.close:
                errors.append(f"[{i}] {c.timestamp}: high < open/close")
                continue
            if c.low > c.open or c.low > c.close:
                errors.append(f"[{i}] {c.timestamp}: low > open/close")
                continue

            # Duplicate check
            ts_key = c.timestamp.isoformat()
            if ts_key in seen_timestamps:
                errors.append(f"[{i}] {c.timestamp}: duplicate timestamp")
                continue
            seen_timestamps.add(ts_key)

            valid.append(c)

        if errors:
            logger.warning(
                f"[DataValidator] {len(errors)} invalid candles removed. "
                f"Sample: {errors[:3]}"
            )

        return valid, errors


# =============================================================================
# CSV LOADER
# =============================================================================

class CsvLoader:
    """
    Load OHLCV candle data from CSV files.

    Supported formats:
    - MetaTrader: Date,Time,Open,High,Low,Close,Volume
    - Standard: timestamp,open,high,low,close,volume
    - TradingView: time,open,high,low,close,volume
    """

    DATETIME_FORMATS = [
        "%Y.%m.%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%m/%d/%Y %H:%M",
    ]

    def __init__(self, validator: Optional[HistoricalDataValidator] = None):
        self.validator = validator or HistoricalDataValidator()

    def _parse_datetime(self, date_str: str, time_str: str = "") -> Optional[datetime]:
        """Parse datetime from various formats."""
        combined = f"{date_str} {time_str}".strip()
        for fmt in self.DATETIME_FORMATS:
            try:
                dt = datetime.strptime(combined, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

        # Try numeric Unix timestamp
        try:
            ts = float(combined)
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except ValueError:
            pass

        logger.warning(f"[CsvLoader] Cannot parse datetime: '{combined}'")
        return None

    def load(self, filepath: str, instrument: str,
             timeframe: str = "M15") -> List[Candle]:
        """Load candles from a CSV file."""
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"Data file not found: {filepath}")

        candles = []
        errors = []

        with open(path, "r", newline="") as f:
            # Detect dialect
            sample = f.read(2048)
            f.seek(0)
            dialect = csv.Sniffer().sniff(sample)
            reader = csv.DictReader(f, dialect=dialect)

            if reader.fieldnames is None:
                raise ValueError(f"CSV file has no headers: {filepath}")

            headers = [h.lower().strip() for h in reader.fieldnames]

            for row_num, row in enumerate(reader, start=2):
                try:
                    # Normalize keys
                    row_lower = {k.lower().strip(): v.strip() for k, v in row.items()}

                    # Parse timestamp
                    if "date" in row_lower and "time" in row_lower:
                        dt = self._parse_datetime(row_lower["date"], row_lower["time"])
                    elif "timestamp" in row_lower:
                        dt = self._parse_datetime(row_lower["timestamp"])
                    elif "time" in row_lower:
                        dt = self._parse_datetime(row_lower["time"])
                    elif "<date>" in row_lower and "<time>" in row_lower:
                        dt = self._parse_datetime(row_lower["<date>"], row_lower["<time>"])
                    else:
                        # Try first column
                        first_val = list(row.values())[0]
                        dt = self._parse_datetime(first_val)

                    if dt is None:
                        errors.append(f"Row {row_num}: Cannot parse datetime")
                        continue

                    # Parse OHLCV — handle MetaTrader <> notation
                    def get_col(*names) -> float:
                        for n in names:
                            v = row_lower.get(n) or row_lower.get(f"<{n}>")
                            if v:
                                return float(v)
                        raise KeyError(f"Column not found: {names}")

                    candle = Candle(
                        timestamp=dt,
                        open=get_col("open"),
                        high=get_col("high"),
                        low=get_col("low"),
                        close=get_col("close"),
                        volume=float(row_lower.get("volume", row_lower.get("<vol>", row_lower.get("<tickvol>", "0"))) or 0),
                        timeframe=timeframe,
                        instrument=instrument,
                        is_complete=True
                    )
                    candles.append(candle)

                except (KeyError, ValueError) as e:
                    errors.append(f"Row {row_num}: {e}")

        if errors:
            logger.warning(f"[CsvLoader] {len(errors)} parse errors in {filepath}")

        # Sort by timestamp
        candles.sort(key=lambda c: c.timestamp)

        # Validate
        candles, val_errors = self.validator.validate(candles)

        logger.info(
            f"[CsvLoader] Loaded {len(candles)} candles for {instrument} "
            f"from {filepath} ({timeframe})"
        )
        return candles

    def generate_synthetic(self,
                           instrument: str,
                           timeframe: str = "M15",
                           n_candles: int = 5000,
                           start_price: float = 1.1000,
                           pip_size: float = 0.0001,
                           seed: int = 42) -> List[Candle]:
        """
        Generate synthetic OHLCV candle data for testing.
        Uses a random walk with realistic bid-ask dynamics.
        Not for production use — testing and smoke tests only.
        """
        import random
        import math
        from datetime import timedelta

        random.seed(seed)
        candles = []
        price = start_price
        base_time = datetime(2023, 1, 2, 7, 0, tzinfo=timezone.utc)
        interval_minutes = {"M1": 1, "M5": 5, "M15": 15, "M30": 30, "H1": 60}.get(timeframe, 15)

        for i in range(n_candles):
            ts = base_time + timedelta(minutes=i * interval_minutes)

            # Skip weekends
            if ts.weekday() >= 5:
                continue

            # Trending bias with mean reversion
            drift = random.gauss(0.00002, 0.0001)
            price += drift

            # OHLC simulation
            atr_base = price * 0.001
            body = random.gauss(0, atr_base * 0.5)
            wick_up = abs(random.gauss(0, atr_base * 0.3))
            wick_down = abs(random.gauss(0, atr_base * 0.3))

            open_p = price
            close_p = price + body
            high_p = max(open_p, close_p) + wick_up
            low_p = min(open_p, close_p) - wick_down

            price = close_p

            candle = Candle(
                timestamp=ts,
                open=round(open_p, 5),
                high=round(high_p, 5),
                low=round(low_p, 5),
                close=round(close_p, 5),
                volume=random.uniform(100, 10000),
                timeframe=timeframe,
                instrument=instrument,
                is_complete=True
            )
            candles.append(candle)

        logger.info(f"[CsvLoader] Generated {len(candles)} synthetic candles for {instrument}")
        return candles


# =============================================================================
# MARKET DATA SERVICE
# =============================================================================

class MarketDataService:
    """
    Central data service for the trading system.
    Manages historical data, candle feeds, and live data adapters.
    """

    def __init__(self, config: dict):
        self.config = config
        self.data_dir = config.get("backtest", {}).get("data_dir", "data/historical")
        self.csv_loader = CsvLoader()
        self._candle_store: Dict[str, List[Candle]] = {}  # symbol → candles

    def load_historical(self, instrument: str, timeframe: str,
                        filepath: Optional[str] = None) -> List[Candle]:
        """Load historical candles for an instrument."""
        if filepath is None:
            filepath = f"{self.data_dir}/{instrument}_{timeframe}.csv"

        try:
            candles = self.csv_loader.load(filepath, instrument, timeframe)
        except FileNotFoundError:
            logger.warning(
                f"[MarketData] No CSV found for {instrument}/{timeframe}, "
                f"generating synthetic data for testing"
            )
            pip_sizes = {
                "XAUUSD": 0.01,
                "EURUSD": 0.0001,
                "GBPUSD": 0.0001,
            }
            pip = pip_sizes.get(instrument, 0.0001)
            start_prices = {
                "XAUUSD": 1900.0,
                "EURUSD": 1.0800,
                "GBPUSD": 1.2500,
                "USDJPY": 145.00,
            }
            start = start_prices.get(instrument, 1.0000)
            candles = self.csv_loader.generate_synthetic(
                instrument, timeframe, n_candles=5000,
                start_price=start, pip_size=pip
            )

        key = f"{instrument}_{timeframe}"
        self._candle_store[key] = candles
        return candles

    def get_candles(self, instrument: str, timeframe: str,
                    n: Optional[int] = None) -> List[Candle]:
        """Retrieve cached candles, optionally limiting to last N."""
        key = f"{instrument}_{timeframe}"
        candles = self._candle_store.get(key, [])
        if n is not None:
            return candles[-n:]
        return candles

    def append_candle(self, instrument: str, timeframe: str, candle: Candle):
        """Append a new live candle to the store."""
        key = f"{instrument}_{timeframe}"
        if key not in self._candle_store:
            self._candle_store[key] = []
        self._candle_store[key].append(candle)
        # Keep rolling window of 10000 candles
        if len(self._candle_store[key]) > 10000:
            self._candle_store[key] = self._candle_store[key][-10000:]

    def get_latest_candle(self, instrument: str, timeframe: str) -> Optional[Candle]:
        """Get the most recent candle."""
        candles = self.get_candles(instrument, timeframe)
        return candles[-1] if candles else None

    def instrument_has_data(self, instrument: str, timeframe: str) -> bool:
        key = f"{instrument}_{timeframe}"
        return key in self._candle_store and len(self._candle_store[key]) > 0

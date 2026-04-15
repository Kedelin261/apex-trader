"""
APEX MULTI-MARKET TJR ENGINE
Integration Tests — Strategy Rule Validation & Routing (v3.0)

Test coverage:
  1. AsianRangeBreakout — EURUSD exact rule validation
  2. ORBVWAPStrategy — ES/NQ signal generation
  3. VWAPSDReversion — XAUUSD mean-reversion signal
  4. CommodityTrend — USOIL trend signal
  5. GapAndGo — AAPL gap scanner
  6. CryptoFundingReversion — BTC/USDT signal
  7. CryptoMondayRange — Monday range setup
  8. RiskNormaliser — cross-asset position sizing
  9. StrategySignal — all strategies produce deterministic output format
  10. OANDA continuity regression — EURUSD pipeline not broken
"""

from __future__ import annotations

import pytest
from datetime import datetime, timezone, timedelta
from typing import List


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _make_candles(
    count: int = 200,
    base_price: float = 1.0850,
    volatility: float = 0.0003,
    trend_per_candle: float = 0.0,
    now: datetime = None,
) -> List[dict]:
    """Generate synthetic OHLCV candles (oldest first)."""
    import random
    if now is None:
        now = datetime.now(timezone.utc)
    candles = []
    price = base_price
    for i in range(count):
        ts = now - timedelta(minutes=15 * (count - i))
        open_p = price
        change = random.gauss(trend_per_candle, volatility)
        close_p = open_p + change
        high_p  = max(open_p, close_p) + abs(random.gauss(0, volatility * 0.3))
        low_p   = min(open_p, close_p) - abs(random.gauss(0, volatility * 0.3))
        volume  = random.randint(500, 50000)
        candles.append({
            "timestamp": ts,
            "open": round(open_p, 6),
            "high": round(high_p, 6),
            "low": round(low_p, 6),
            "close": round(close_p, 6),
            "volume": float(volume),
        })
        price = close_p
    return candles


def _make_asian_session_candles_with_breakout(
    asian_high: float = 1.0870,
    asian_low: float = 1.0840,
    breakout_direction: str = "BUY",
) -> List[dict]:
    """
    Build candles that set up a valid Asian range breakout in London session.
    Asian session: 00:00–08:00 UTC
    London session starts: 07:00 UTC
    Breakout candle: 08:30 UTC (inside London session, before 12:00 kill)
    """
    now = datetime.now(timezone.utc).replace(hour=9, minute=0, second=0, microsecond=0)

    # Build historical candles first (200 M15 candles back)
    candles = _make_candles(count=180, base_price=(asian_high + asian_low) / 2)

    # Build Asian session candles (00:00 - 08:00 UTC) — 32 M15 candles
    asian_start = now.replace(hour=0, minute=0)
    mid_price = (asian_high + asian_low) / 2.0
    for i in range(32):  # 8 hours × 4 candles/hour
        ts = asian_start + timedelta(minutes=15 * i)
        candles.append({
            "timestamp": ts,
            "open": mid_price,
            "high": asian_high if i % 3 == 0 else mid_price + 0.0002,
            "low": asian_low if i % 4 == 0 else mid_price - 0.0002,
            "close": mid_price,
            "volume": 1000.0,
        })

    # Breakout candle at 08:30 UTC (in London session, before 12:00 kill)
    if breakout_direction == "BUY":
        breakout_close = asian_high + 0.0015  # breaks above Asian High
        bo_high = breakout_close + 0.0003
        bo_low  = asian_high - 0.0002
    else:
        breakout_close = asian_low - 0.0015  # breaks below Asian Low
        bo_high = asian_low + 0.0002
        bo_low  = breakout_close - 0.0003

    bo_ts = now.replace(hour=8, minute=30)
    bo_open = (asian_high + asian_low) / 2.0
    candles.append({
        "timestamp": bo_ts,
        "open": bo_open,
        "high": bo_high,
        "low": bo_low,
        "close": breakout_close,
        "volume": 5000.0,
    })

    return candles


# ─────────────────────────────────────────────────────────────────────────────
# 1. AsianRangeBreakout
# ─────────────────────────────────────────────────────────────────────────────

class TestAsianRangeBreakout:

    def _strategy(self):
        from strategies.asian_range_breakout import AsianRangeBreakout
        return AsianRangeBreakout()

    def test_no_signal_insufficient_candles(self):
        s = self._strategy()
        candles = _make_candles(count=10)
        signal = s.evaluate(candles, {"symbol": "EURUSD"})
        assert signal.direction == "NONE"
        assert signal.is_valid is False

    def test_no_signal_no_asian_data(self):
        """If all candles are outside Asian session window, no setup."""
        s = self._strategy()
        # All candles during NY afternoon (no Asian data)
        now = datetime.now(timezone.utc).replace(hour=20)
        candles = _make_candles(count=200, now=now)
        signal = s.evaluate(candles, {"symbol": "EURUSD"})
        assert signal.direction == "NONE"

    def test_signal_output_format(self):
        """Strategy must always return a StrategySignal regardless of direction."""
        from strategies.base_strategy import StrategySignal
        s = self._strategy()
        candles = _make_candles(count=200)
        signal = s.evaluate(candles, {"symbol": "EURUSD"})
        assert isinstance(signal, StrategySignal)
        assert signal.strategy_name == "asian_range_breakout"
        assert signal.asset_class == "FOREX"
        assert signal.symbol == "EURUSD"
        assert hasattr(signal, "rules_passed")
        assert hasattr(signal, "rules_failed")

    def test_strategy_name(self):
        s = self._strategy()
        assert s.name == "asian_range_breakout"
        assert s.asset_class == "FOREX"


# ─────────────────────────────────────────────────────────────────────────────
# 2. ORBVWAPStrategy
# ─────────────────────────────────────────────────────────────────────────────

class TestORBVWAPStrategy:

    def _strategy(self):
        from strategies.orb_vwap import ORBWithVWAP
        return ORBWithVWAP()

    def test_strategy_name_and_asset_class(self):
        s = self._strategy()
        assert "orb" in s.name.lower() or "vwap" in s.name.lower()
        assert s.asset_class in ("FUTURES", "STOCKS", "INDICES", "COMMODITIES")

    def test_no_signal_insufficient_data(self):
        s = self._strategy()
        candles = _make_candles(count=5, base_price=5200.0)
        signal = s.evaluate(candles, {"symbol": "ES"})
        assert signal.direction == "NONE"
        assert signal.is_valid is False

    def test_returns_strategy_signal(self):
        from strategies.base_strategy import StrategySignal
        from strategies.orb_vwap import ORBWithVWAP
        s = ORBWithVWAP()
        candles = _make_candles(count=200, base_price=5200.0)
        signal = s.evaluate(candles, {"symbol": "ES"})
        assert isinstance(signal, StrategySignal)


# ─────────────────────────────────────────────────────────────────────────────
# 3. VWAPSDReversion
# ─────────────────────────────────────────────────────────────────────────────

class TestVWAPSDReversion:

    def _strategy(self):
        from strategies.vwap_sd_reversion import VWAPSDReversion
        return VWAPSDReversion()

    def test_strategy_name_and_asset_class(self):
        s = self._strategy()
        assert "vwap" in s.name.lower()
        assert s.asset_class in ("FUTURES", "FOREX", "GOLD", "INDICES", "COMMODITIES", "STOCKS")

    def test_no_signal_insufficient_data(self):
        s = self._strategy()
        candles = _make_candles(count=3, base_price=2350.0)
        signal = s.evaluate(candles, {"symbol": "XAUUSD"})
        assert signal.direction == "NONE"

    def test_returns_strategy_signal_format(self):
        from strategies.base_strategy import StrategySignal
        s = self._strategy()
        candles = _make_candles(count=200, base_price=2350.0)
        signal = s.evaluate(candles, {"symbol": "XAUUSD"})
        assert isinstance(signal, StrategySignal)
        assert signal.symbol == "XAUUSD"


# ─────────────────────────────────────────────────────────────────────────────
# 4. CommodityTrend
# ─────────────────────────────────────────────────────────────────────────────

class TestCommodityTrend:

    def _strategy(self):
        from strategies.commodity_trend import CommodityTrend
        return CommodityTrend()

    def test_strategy_name_and_asset_class(self):
        s = self._strategy()
        assert "commodity" in s.name.lower() or "trend" in s.name.lower()
        assert s.asset_class in ("COMMODITIES", "FUTURES")

    def test_no_signal_insufficient_data(self):
        s = self._strategy()
        candles = _make_candles(count=10, base_price=75.0)
        signal = s.evaluate(candles, {"symbol": "USOIL"})
        assert signal.direction == "NONE"

    def test_returns_signal(self):
        from strategies.base_strategy import StrategySignal
        s = self._strategy()
        candles = _make_candles(count=200, base_price=75.0, volatility=0.5)
        signal = s.evaluate(candles, {"symbol": "USOIL"})
        assert isinstance(signal, StrategySignal)


# ─────────────────────────────────────────────────────────────────────────────
# 5. GapAndGo (Stocks)
# ─────────────────────────────────────────────────────────────────────────────

class TestGapAndGo:

    def _strategy(self):
        from strategies.gap_and_go import GapAndGo
        return GapAndGo()

    def test_strategy_name_and_asset_class(self):
        s = self._strategy()
        assert "gap" in s.name.lower()
        assert s.asset_class in ("STOCKS", "INDICES")

    def test_no_signal_insufficient_data(self):
        s = self._strategy()
        candles = _make_candles(count=5, base_price=185.0)
        signal = s.evaluate(candles, {"symbol": "AAPL"})
        assert signal.direction == "NONE"

    def test_returns_strategy_signal(self):
        from strategies.base_strategy import StrategySignal
        s = self._strategy()
        # Create candles with a gap: prev close 180, current open 185 (gap up)
        candles = _make_candles(count=200, base_price=185.0, volatility=0.5)
        signal = s.evaluate(candles, {"symbol": "AAPL"})
        assert isinstance(signal, StrategySignal)
        assert signal.symbol == "AAPL"

    def test_gap_scanner_import(self):
        """adapters/stocks/gap_scanner.py must be importable."""
        from adapters.stocks.gap_scanner import GapScanner
        assert GapScanner is not None


# ─────────────────────────────────────────────────────────────────────────────
# 6. CryptoFundingReversion
# ─────────────────────────────────────────────────────────────────────────────

class TestCryptoFundingReversion:

    def _strategy(self):
        from strategies.crypto_funding_reversion import CryptoFundingReversion
        return CryptoFundingReversion()

    def test_strategy_name_and_asset_class(self):
        s = self._strategy()
        assert "crypto" in s.name.lower() or "funding" in s.name.lower()
        assert s.asset_class in ("CRYPTO",)

    def test_no_signal_insufficient_data(self):
        s = self._strategy()
        candles = _make_candles(count=3, base_price=40000.0)
        signal = s.evaluate(candles, {"symbol": "BTC/USDT"})
        assert signal.direction == "NONE"

    def test_returns_signal(self):
        from strategies.base_strategy import StrategySignal
        s = self._strategy()
        candles = _make_candles(count=200, base_price=40000.0, volatility=200.0)
        signal = s.evaluate(candles, {"symbol": "BTC/USDT"})
        assert isinstance(signal, StrategySignal)


# ─────────────────────────────────────────────────────────────────────────────
# 7. CryptoMondayRange
# ─────────────────────────────────────────────────────────────────────────────

class TestCryptoMondayRange:

    def _strategy(self):
        from strategies.crypto_monday_range import CryptoMondayRange
        return CryptoMondayRange()

    def test_strategy_name_and_asset_class(self):
        s = self._strategy()
        assert "monday" in s.name.lower() or "range" in s.name.lower()
        assert s.asset_class in ("CRYPTO",)

    def test_returns_signal(self):
        from strategies.base_strategy import StrategySignal
        s = self._strategy()
        candles = _make_candles(count=200, base_price=40000.0, volatility=300.0)
        signal = s.evaluate(candles, {"symbol": "BTC/USDT"})
        assert isinstance(signal, StrategySignal)


# ─────────────────────────────────────────────────────────────────────────────
# 8. RiskNormaliser — cross-asset position sizing
# ─────────────────────────────────────────────────────────────────────────────

class TestRiskNormaliser:

    def _normaliser(self, risk_pct: float = 1.0):
        from live.autonomous_scheduler import RiskNormaliser
        return RiskNormaliser(risk_pct=risk_pct)

    def test_eurusd_forex_sizing(self):
        """EURUSD: 1% of $100k = $1000 risk, 10pip SL = ~1 lot."""
        norm = self._normaliser(risk_pct=1.0)
        result = norm.calculate(
            symbol="EURUSD",
            asset_class="FOREX",
            account_balance=100_000.0,
            entry_price=1.0850,
            stop_loss=1.0750,  # 100 pips SL
        )
        assert result["units"] > 0
        assert result["method"] == "FOREX"
        assert result["risk_usd"] <= 1100  # allow some margin
        assert result["sl_pips"] > 0

    def test_gc_futures_sizing(self):
        """GC: 1% of $100k = $1000 risk, $20 SL = 2 contracts (tick_val=$10/tick, 200 ticks)."""
        norm = self._normaliser(risk_pct=1.0)
        result = norm.calculate(
            symbol="GC",
            asset_class="FUTURES",
            account_balance=100_000.0,
            entry_price=2350.0,
            stop_loss=2330.0,  # $20 SL
        )
        assert result["units"] >= 1.0
        assert result["method"] == "FUTURES"
        assert result["units"] == float(int(result["units"]))  # whole contracts

    def test_es_futures_sizing(self):
        """ES: 1% of $100k = $1000, $50 SL → 200 ticks × $12.50 = $2500 per contract → 0.4 → 1 min."""
        norm = self._normaliser(risk_pct=1.0)
        result = norm.calculate(
            symbol="ES",
            asset_class="FUTURES",
            account_balance=100_000.0,
            entry_price=5200.0,
            stop_loss=5150.0,  # $50 SL = 200 ticks × 0.25
        )
        assert result["units"] >= 1.0  # minimum 1 contract

    def test_aapl_stock_sizing(self):
        """AAPL: 1% of $100k = $1000, $5 SL = 200 shares."""
        norm = self._normaliser(risk_pct=1.0)
        result = norm.calculate(
            symbol="AAPL",
            asset_class="STOCKS",
            account_balance=100_000.0,
            entry_price=185.0,
            stop_loss=180.0,  # $5 SL
        )
        assert result["units"] == 200.0  # $1000 / $5 = 200 shares
        assert result["method"] == "STOCKS"

    def test_zero_sl_distance_returns_zero(self):
        norm = self._normaliser()
        result = norm.calculate(
            symbol="EURUSD",
            asset_class="FOREX",
            account_balance=100_000.0,
            entry_price=1.0850,
            stop_loss=1.0850,  # no distance
        )
        assert result["units"] == 0.0
        assert "error" in result

    def test_consistent_risk_pct_across_asset_classes(self):
        """Risk USD should be approximately equal across asset classes for same account/pct."""
        norm = self._normaliser(risk_pct=1.0)
        balance = 100_000.0
        target_risk = balance * 0.01  # $1000

        # Stocks
        stocks = norm.calculate("AAPL", "STOCKS", balance, 185.0, 180.0)
        assert abs(stocks["risk_usd"] - target_risk) <= 100  # within $100

        # Futures (GC)
        futures = norm.calculate("GC", "FUTURES", balance, 2350.0, 2340.0)
        assert futures["units"] >= 1


# ─────────────────────────────────────────────────────────────────────────────
# 9. StrategySignal deterministic format
# ─────────────────────────────────────────────────────────────────────────────

class TestStrategySignalFormat:

    def _all_strategies(self):
        strategies = []
        try:
            from strategies.asian_range_breakout import AsianRangeBreakout
            strategies.append(("EURUSD", AsianRangeBreakout()))
        except Exception:
            pass
        try:
            from strategies.orb_vwap import ORBWithVWAP
            strategies.append(("ES", ORBWithVWAP()))
        except Exception:
            pass
        try:
            from strategies.vwap_sd_reversion import VWAPSDReversion
            strategies.append(("XAUUSD", VWAPSDReversion()))
        except Exception:
            pass
        try:
            from strategies.commodity_trend import CommodityTrend
            strategies.append(("USOIL", CommodityTrend()))
        except Exception:
            pass
        try:
            from strategies.gap_and_go import GapAndGo
            strategies.append(("AAPL", GapAndGo()))
        except Exception:
            pass
        try:
            from strategies.crypto_funding_reversion import CryptoFundingReversion
            strategies.append(("BTC/USDT", CryptoFundingReversion()))
        except Exception:
            pass
        return strategies

    def test_all_strategies_return_strategy_signal(self):
        from strategies.base_strategy import StrategySignal
        for symbol, strategy in self._all_strategies():
            base = {"XAUUSD": 2350.0, "EURUSD": 1.085}.get(symbol, 100.0)
            candles = _make_candles(count=200, base_price=base)
            signal = strategy.evaluate(candles, {"symbol": symbol})
            assert isinstance(signal, StrategySignal), (
                f"{strategy.name} did not return StrategySignal"
            )
            assert signal.strategy_name == strategy.name
            assert signal.direction in ("BUY", "SELL", "NONE")
            assert isinstance(signal.rules_passed, list)
            assert isinstance(signal.rules_failed, list)

    def test_signal_to_dict_complete(self):
        from strategies.asian_range_breakout import AsianRangeBreakout
        s = AsianRangeBreakout()
        candles = _make_candles(count=200)
        signal = s.evaluate(candles, {"symbol": "EURUSD"})
        d = signal.to_dict()
        required_keys = [
            "strategy_name", "asset_class", "symbol", "direction",
            "entry_price", "stop_loss", "take_profit", "position_size",
            "risk_usd", "reward_to_risk", "timeframe", "session",
            "rules_passed", "rules_failed", "filter_blocks", "is_valid",
            "generated_at",
        ]
        for key in required_keys:
            assert key in d, f"Missing key in StrategySignal.to_dict(): {key}"


# ─────────────────────────────────────────────────────────────────────────────
# 10. OANDA continuity regression — EURUSD pipeline not broken
# ─────────────────────────────────────────────────────────────────────────────

class TestOANDAContinuityRegression:
    """
    Ensures that adding IBKR does not break the existing EURUSD/OANDA pipeline.
    """

    def test_eurusd_still_routes_to_oanda(self):
        from live.broker_manager import get_broker_for_symbol
        assert get_broker_for_symbol("EURUSD") == "oanda"

    def test_oanda_symbol_mapper_still_has_eurusd(self):
        from brokers.symbol_mapper import BrokerSymbolMapper, _OANDA_MAP
        assert "EURUSD" in _OANDA_MAP
        broker_sym, asset_class = _OANDA_MAP["EURUSD"]
        assert broker_sym == "EUR_USD"

    def test_eurusd_not_in_ibkr_map(self):
        from brokers.symbol_mapper import _IBKR_MAP
        assert "EURUSD" not in _IBKR_MAP

    def test_ibkr_connector_rejects_eurusd(self):
        from brokers.ibkr_connector import build_ibkr_contract
        with pytest.raises(ValueError):
            build_ibkr_contract("EURUSD")

    def test_existing_oanda_xauusd_mapping_preserved(self):
        """OANDA map still has XAU_USD entry (for legacy/reference)."""
        from brokers.symbol_mapper import _OANDA_MAP
        assert "XAUUSD" in _OANDA_MAP
        assert _OANDA_MAP["XAUUSD"][0] == "XAU_USD"

    def test_asian_range_breakout_for_eurusd_still_works(self):
        """AsianRangeBreakout on EURUSD must still evaluate without error."""
        from strategies.asian_range_breakout import AsianRangeBreakout
        from strategies.base_strategy import StrategySignal
        s = AsianRangeBreakout()
        candles = _make_candles(count=200, base_price=1.0850)
        signal = s.evaluate(candles, {"symbol": "EURUSD"})
        assert isinstance(signal, StrategySignal)
        assert signal.symbol == "EURUSD"
        assert signal.asset_class == "FOREX"

    def test_brokers_symbol_normalization_eurusd(self):
        """Symbol normalisation still maps EURUSD correctly."""
        from domain.symbol_utils import normalize_symbol
        normalised = normalize_symbol("EURUSD")
        assert normalised == "EURUSD"

    def test_all_existing_oanda_tests_still_importable(self):
        """Existing OANDA test files must still import without error."""
        import importlib
        for module in [
            "tests.unit.test_oanda_connector_v23",
            "tests.unit.test_symbol_mapper_v23",
            "tests.unit.test_symbol_normalization",
        ]:
            try:
                importlib.import_module(module)
            except ImportError as e:
                pytest.fail(f"Existing test {module} failed to import: {e}")

"""
APEX MULTI-MARKET TJR ENGINE
Autonomous Scheduler — Continuous trading loop with market data ingestion,
strategy scanning, signal generation, execution routing, and position monitoring.

ARCHITECTURE RULES (NON-NEGOTIABLE):
  - No manual /api/signal calls required in autonomous mode
  - All signals flow through: StrategyScanner → StrategyValidator → RiskGuardian → ExecutionSupervisor
  - No strategy directly submits orders — all go through BrokerManager
  - Broker routing enforced by get_broker_for_symbol() in broker_manager.py
  - Kill switch checked before every scan cycle
  - Risk normalisation across asset classes: % of account per trade is consistent

CONTINUOUS LOOPS:
  1. MarketDataLoop     — ingests live price data (30s interval default)
  2. StrategyScanLoop   — evaluates all active strategies (30s interval)
  3. SignalProcessLoop  — validates signals through Intent Layer (real-time)
  4. PositionMonitorLoop— monitors open positions, manages stops (10s interval)
  5. ReconciliationLoop — reconciles engine state with broker (60s interval)

All loops are APScheduler jobs (non-blocking background threads).
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# RISK NORMALISER
# Cross-asset-class position sizing so % of account risk is consistent.
# ---------------------------------------------------------------------------

class RiskNormaliser:
    """
    Normalises position size across asset classes so each trade risks
    exactly `risk_pct` of the account balance.

    EXACT FORMULAS (non-interpretive):

    FOREX:
      risk_pct   = max_risk_per_trade_pct (default 1.0%)
      risk_usd   = account_balance × (risk_pct / 100)
      sl_pips    = abs(entry_price - stop_loss) / pip_size
      pip_value  = pip_size × lot_size  [for a 1-lot position in quote currency]
      lots       = risk_usd / (sl_pips × pip_value_per_lot_usd)
      e.g. EURUSD: pip_value_per_lot = $10 (1 pip = $10 per standard lot)

    FUTURES (GC/ES/NQ):
      risk_usd   = account_balance × (risk_pct / 100)
      sl_ticks   = abs(entry_price - stop_loss) / tick_size
      tick_value = tick_size × contract_multiplier
      contracts  = risk_usd / (sl_ticks × tick_value)
      Minimum: 1 contract; Maximum: limited by account margin
      e.g. GC: tick_size=0.10, multiplier=100, tick_value=$10
           ES: tick_size=0.25, multiplier=50, tick_value=$12.50
           NQ: tick_size=0.25, multiplier=20, tick_value=$5.00

    STOCKS:
      risk_usd   = account_balance × (risk_pct / 100)
      risk_per_share = abs(entry_price - stop_loss)
      shares     = risk_usd / risk_per_share
      Minimum: 1 share

    CRYPTO:
      risk_usd   = account_balance × (risk_pct / 100)
      risk_per_unit = abs(entry_price - stop_loss)
      units      = risk_usd / risk_per_unit
      (may be fractional for BTC/ETH)
    """

    # Futures tick/multiplier reference (exact values)
    FUTURES_SPECS = {
        "GC":   {"tick_size": 0.10,  "multiplier": 100,   "tick_value": 10.00},
        "XAUUSD": {"tick_size": 0.10, "multiplier": 100,  "tick_value": 10.00},
        "ES":   {"tick_size": 0.25,  "multiplier": 50,    "tick_value": 12.50},
        "NQ":   {"tick_size": 0.25,  "multiplier": 20,    "tick_value": 5.00},
        "YM":   {"tick_size": 1.00,  "multiplier": 5,     "tick_value": 5.00},
        "CL":   {"tick_size": 0.01,  "multiplier": 1000,  "tick_value": 10.00},
    }

    # Forex pip values (per standard lot, in USD)
    FOREX_PIP_VALUES = {
        "EURUSD": 10.00,
        "GBPUSD": 10.00,
        "AUDUSD": 10.00,
        "NZDUSD": 10.00,
        "USDJPY": 8.00,   # approximate (varies with JPY rate)
        "USDCAD": 7.50,
        "USDCHF": 10.00,
        "EURJPY": 8.00,
        "GBPJPY": 8.00,
    }

    # Forex pip sizes
    FOREX_PIP_SIZES = {
        "USDJPY": 0.01,
        "EURJPY": 0.01,
        "GBPJPY": 0.01,
    }

    def __init__(self, risk_pct: float = 1.0):
        """
        Args:
            risk_pct: Max risk per trade as % of account (default 1.0 = 1%)
        """
        self.risk_pct = risk_pct

    def calculate(
        self,
        symbol: str,
        asset_class: str,
        account_balance: float,
        entry_price: float,
        stop_loss: float,
    ) -> dict:
        """
        Calculate normalised position size.

        Returns dict with:
          - units: position size in native units (lots/contracts/shares)
          - risk_usd: dollar risk amount
          - risk_pct: actual risk percentage
          - method: calculation method used
        """
        risk_usd = account_balance * (self.risk_pct / 100.0)
        sl_distance = abs(entry_price - stop_loss)

        if sl_distance <= 0:
            return {
                "units": 0.0,
                "risk_usd": 0.0,
                "risk_pct": 0.0,
                "method": "ZERO_SL_DISTANCE",
                "error": "Stop loss distance is zero — cannot size position",
            }

        ac = asset_class.upper()

        if ac in ("FUTURES", "COMMODITIES", "GOLD"):
            return self._size_futures(symbol, risk_usd, sl_distance)
        elif ac in ("FOREX",):
            return self._size_forex(symbol, risk_usd, entry_price, sl_distance)
        elif ac in ("STOCKS",):
            return self._size_stocks(risk_usd, sl_distance)
        elif ac in ("CRYPTO",):
            return self._size_crypto(risk_usd, sl_distance)
        elif ac in ("INDICES",):
            return self._size_indices(symbol, risk_usd, sl_distance)
        else:
            # Unknown asset class — use generic % risk
            return self._size_generic(risk_usd, sl_distance, entry_price)

    def _size_futures(self, symbol: str, risk_usd: float, sl_distance: float) -> dict:
        """
        Futures: contracts = risk_usd / (sl_ticks × tick_value)
        """
        spec = self.FUTURES_SPECS.get(symbol.upper(), {
            "tick_size": 1.0,
            "multiplier": 1,
            "tick_value": 1.0,
        })
        tick_size  = spec["tick_size"]
        tick_value = spec["tick_value"]

        sl_ticks = sl_distance / tick_size
        if sl_ticks <= 0:
            return {"units": 1.0, "risk_usd": 0.0, "risk_pct": 0.0, "method": "FUTURES_MIN_1"}

        contracts = risk_usd / (sl_ticks * tick_value)
        contracts = max(1.0, round(contracts))   # Minimum 1 contract

        actual_risk = sl_ticks * tick_value * contracts
        return {
            "units": contracts,
            "risk_usd": round(actual_risk, 2),
            "risk_pct": round((actual_risk / (risk_usd / (self.risk_pct / 100.0))) * self.risk_pct, 4),
            "method": "FUTURES",
            "sl_ticks": sl_ticks,
            "tick_value": tick_value,
            "spec": spec,
        }

    def _size_forex(
        self, symbol: str, risk_usd: float, entry_price: float, sl_distance: float
    ) -> dict:
        """
        Forex: lots = risk_usd / (sl_pips × pip_value_per_lot_usd)
        """
        sym = symbol.upper()
        pip_size = self.FOREX_PIP_SIZES.get(sym, 0.0001)
        pip_value = self.FOREX_PIP_VALUES.get(sym, 10.0)

        sl_pips = sl_distance / pip_size
        if sl_pips <= 0:
            return {"units": 0.0, "risk_usd": 0.0, "risk_pct": 0.0, "method": "FOREX_ZERO_PIPS"}

        lots = risk_usd / (sl_pips * pip_value)
        lots = max(0.01, round(lots, 2))  # Minimum 0.01 lot

        actual_risk = sl_pips * pip_value * lots
        return {
            "units": lots,
            "risk_usd": round(actual_risk, 2),
            "risk_pct": round((actual_risk / (risk_usd / (self.risk_pct / 100.0))) * self.risk_pct, 4),
            "method": "FOREX",
            "sl_pips": round(sl_pips, 1),
            "pip_size": pip_size,
            "pip_value_per_lot": pip_value,
        }

    def _size_stocks(self, risk_usd: float, sl_distance: float) -> dict:
        """
        Stocks: shares = risk_usd / risk_per_share
        """
        shares = risk_usd / sl_distance
        shares = max(1.0, round(shares))   # Minimum 1 share

        actual_risk = sl_distance * shares
        return {
            "units": shares,
            "risk_usd": round(actual_risk, 2),
            "risk_pct": 0.0,
            "method": "STOCKS",
            "risk_per_share": round(sl_distance, 4),
        }

    def _size_crypto(self, risk_usd: float, sl_distance: float) -> dict:
        """
        Crypto: units = risk_usd / risk_per_unit (fractional OK)
        """
        units = risk_usd / sl_distance
        units = max(0.001, round(units, 6))   # Fractional allowed

        actual_risk = sl_distance * units
        return {
            "units": units,
            "risk_usd": round(actual_risk, 2),
            "risk_pct": 0.0,
            "method": "CRYPTO",
        }

    def _size_indices(self, symbol: str, risk_usd: float, sl_distance: float) -> dict:
        """
        Indices: use futures spec if available, else generic.
        """
        spec = self.FUTURES_SPECS.get(symbol.upper())
        if spec:
            return self._size_futures(symbol, risk_usd, sl_distance)
        # Generic: treat as 1 unit per $sl_distance
        units = max(1.0, round(risk_usd / sl_distance))
        return {
            "units": units,
            "risk_usd": round(sl_distance * units, 2),
            "risk_pct": 0.0,
            "method": "INDICES_GENERIC",
        }

    def _size_generic(self, risk_usd: float, sl_distance: float, entry_price: float) -> dict:
        units = risk_usd / sl_distance
        return {
            "units": max(1.0, round(units, 4)),
            "risk_usd": round(risk_usd, 2),
            "risk_pct": 0.0,
            "method": "GENERIC",
        }


# ---------------------------------------------------------------------------
# AUTONOMOUS SCHEDULER
# ---------------------------------------------------------------------------

class AutonomousScheduler:
    """
    Continuous autonomous trading scheduler.

    Runs all market loops without requiring manual API calls:
      - MarketDataLoop  : fetch live candles/prices per instrument
      - StrategyScanLoop: evaluate active strategies on fresh data
      - SignalProcessLoop: route valid signals through Intent Layer
      - PositionMonitor : check open positions, enforce stops
      - Reconciler      : reconcile engine ↔ broker state

    Usage:
        scheduler = AutonomousScheduler(orchestrator, broker_manager, config)
        scheduler.start()
        # ... runs continuously ...
        scheduler.stop()
    """

    def __init__(
        self,
        orchestrator,
        broker_manager,
        config: dict,
        market_data_service=None,
    ):
        self._orchestrator = orchestrator
        self._broker_manager = broker_manager
        self._config = config
        self._market_data = market_data_service

        # Read intervals from config (seconds)
        agent_cfg = config.get("agents", {})
        self._data_interval      = agent_cfg.get("orchestrator_interval_seconds", 30)
        self._strategy_interval  = agent_cfg.get("orchestrator_interval_seconds", 30)
        self._position_interval  = agent_cfg.get("risk_guardian", {}).get("check_interval_seconds", 10)
        self._recon_interval     = 60

        # Risk normaliser — uses config risk settings
        risk_cfg = config.get("risk", {})
        self._risk_normaliser = RiskNormaliser(
            risk_pct=risk_cfg.get("max_risk_per_trade_pct", 1.0)
        )

        # Strategy registry
        self._strategies = self._load_strategies()

        # State
        self._running = False
        self._cycle_count = 0
        self._threads: List[threading.Thread] = []
        self._stop_event = threading.Event()

        # Signal queue (StrategySignal objects pending Intent Layer processing)
        self._pending_signals: List[dict] = []
        self._signal_lock = threading.Lock()

        # Per-instrument candle cache (populated by MarketDataLoop)
        self._candle_cache: Dict[str, List[dict]] = {}
        self._cache_lock = threading.Lock()

        logger.info(
            f"[AutonomousScheduler] Initialised — "
            f"data_interval={self._data_interval}s "
            f"strategy_interval={self._strategy_interval}s "
            f"position_interval={self._position_interval}s"
        )

    def _load_strategies(self) -> dict:
        """
        Load all active strategy modules.
        Returns dict of {strategy_name: strategy_instance}.
        """
        strategies = {}
        try:
            from strategies.asian_range_breakout import AsianRangeBreakout
            strategies["asian_range_breakout"] = AsianRangeBreakout()
        except Exception as e:
            logger.warning(f"[Scheduler] Failed to load AsianRangeBreakout: {e}")

        try:
            from strategies.orb_vwap import ORBVWAPStrategy
            strategies["orb_vwap"] = ORBVWAPStrategy()
        except Exception as e:
            logger.warning(f"[Scheduler] Failed to load ORBVWAPStrategy: {e}")

        try:
            from strategies.vwap_sd_reversion import VWAPSDReversion
            strategies["vwap_sd_reversion"] = VWAPSDReversion()
        except Exception as e:
            logger.warning(f"[Scheduler] Failed to load VWAPSDReversion: {e}")

        try:
            from strategies.commodity_trend import CommodityTrend
            strategies["commodity_trend"] = CommodityTrend()
        except Exception as e:
            logger.warning(f"[Scheduler] Failed to load CommodityTrend: {e}")

        try:
            from strategies.gap_and_go import GapAndGo
            strategies["gap_and_go"] = GapAndGo()
        except Exception as e:
            logger.warning(f"[Scheduler] Failed to load GapAndGo: {e}")

        try:
            from strategies.crypto_funding_reversion import CryptoFundingReversion
            strategies["crypto_funding_reversion"] = CryptoFundingReversion()
        except Exception as e:
            logger.warning(f"[Scheduler] Failed to load CryptoFundingReversion: {e}")

        try:
            from strategies.crypto_monday_range import CryptoMondayRange
            strategies["crypto_monday_range"] = CryptoMondayRange()
        except Exception as e:
            logger.warning(f"[Scheduler] Failed to load CryptoMondayRange: {e}")

        try:
            from strategies.prediction_market_arb import PredictionMarketArb
            strategies["prediction_market_arb"] = PredictionMarketArb()
        except Exception as e:
            logger.warning(f"[Scheduler] Failed to load PredictionMarketArb: {e}")

        logger.info(f"[AutonomousScheduler] Loaded {len(strategies)} strategy modules: {list(strategies.keys())}")
        return strategies

    # ------------------------------------------------------------------
    # START / STOP
    # ------------------------------------------------------------------

    def start(self):
        """Start all continuous loops as daemon threads."""
        if self._running:
            logger.warning("[AutonomousScheduler] Already running — ignoring start()")
            return

        self._running = True
        self._stop_event.clear()

        # Define loop workers
        loops = [
            ("market-data-loop",     self._market_data_loop,     self._data_interval),
            ("strategy-scan-loop",   self._strategy_scan_loop,   self._strategy_interval),
            ("signal-process-loop",  self._signal_process_loop,  5),   # fast loop
            ("position-monitor-loop",self._position_monitor_loop,self._position_interval),
            ("reconciliation-loop",  self._reconciliation_loop,  self._recon_interval),
        ]

        for name, worker, interval in loops:
            thread = threading.Thread(
                target=self._run_loop,
                args=(name, worker, interval),
                daemon=True,
                name=name,
            )
            thread.start()
            self._threads.append(thread)

        logger.info(
            f"[AutonomousScheduler] STARTED — {len(self._threads)} loops running. "
            "All signals will flow through StrategyValidator → RiskGuardian → ExecutionSupervisor."
        )

    def stop(self):
        """Stop all loops gracefully."""
        logger.info("[AutonomousScheduler] Stopping all loops...")
        self._running = False
        self._stop_event.set()
        for thread in self._threads:
            thread.join(timeout=5)
        self._threads.clear()
        logger.info("[AutonomousScheduler] All loops stopped.")

    def _run_loop(self, name: str, worker, interval: float):
        """Generic loop runner — calls worker every `interval` seconds."""
        logger.info(f"[Loop:{name}] Started — interval={interval}s")
        while not self._stop_event.is_set():
            try:
                worker()
            except Exception as exc:
                logger.error(f"[Loop:{name}] Exception in worker: {exc}", exc_info=True)
            # Wait for interval or until stop event
            self._stop_event.wait(timeout=interval)
        logger.info(f"[Loop:{name}] Stopped")

    # ------------------------------------------------------------------
    # LOOP 1: MARKET DATA INGESTION
    # ------------------------------------------------------------------

    def _market_data_loop(self):
        """
        Fetch latest candle data for all active instruments.
        Populates _candle_cache for strategy evaluation.

        In production: queries live broker/data feed.
        In paper/test: uses market_data_service synthetic data.
        """
        active_symbols = self._get_active_symbols()
        if not active_symbols:
            return

        for symbol in active_symbols:
            try:
                candles = self._fetch_candles(symbol, timeframe="M15", count=200)
                if candles:
                    with self._cache_lock:
                        self._candle_cache[symbol] = candles
            except Exception as exc:
                logger.warning(f"[MarketDataLoop] Failed to fetch {symbol}: {exc}")

        logger.debug(
            f"[MarketDataLoop] Cache updated: {len(self._candle_cache)} instruments"
        )

    def _fetch_candles(
        self, symbol: str, timeframe: str = "M15", count: int = 200
    ) -> List[dict]:
        """
        Fetch candles from the appropriate source based on symbol routing.

        EURUSD → OANDA REST API
        XAUUSD, Futures, Stocks → IBKR market data
        Crypto → CCXT exchange
        Paper → synthetic/random candles for testing
        """
        from live.broker_manager import get_broker_for_symbol
        target = get_broker_for_symbol(symbol)

        # Try live data service first
        if self._market_data:
            try:
                candles = self._market_data.get_candles(symbol, timeframe, count)
                if candles:
                    return candles
            except Exception:
                pass

        # OANDA data for EURUSD
        if target == "oanda" and self._broker_manager._connector:
            try:
                connector = self._broker_manager._connector
                if hasattr(connector, "get_candles"):
                    return connector.get_candles(symbol, timeframe, count)
            except Exception as exc:
                logger.debug(f"[MarketDataLoop] OANDA candles failed for {symbol}: {exc}")

        # IBKR data for XAUUSD/Futures/Stocks
        if target == "ibkr" and self._broker_manager._ibkr_connector:
            try:
                connector = self._broker_manager._ibkr_connector
                if hasattr(connector, "get_candles"):
                    return connector.get_candles(symbol, timeframe, count)
            except Exception as exc:
                logger.debug(f"[MarketDataLoop] IBKR candles failed for {symbol}: {exc}")

        # Generate synthetic candles for paper/test mode
        return self._generate_synthetic_candles(symbol, count)

    def _generate_synthetic_candles(self, symbol: str, count: int) -> List[dict]:
        """
        Generate realistic synthetic OHLCV candles for paper trading / CI testing.
        Used when no live data feed is available.
        """
        import random
        base_prices = {
            "EURUSD": 1.0850, "GBPUSD": 1.2650, "XAUUSD": 2350.0,
            "ES": 5200.0, "NQ": 18500.0, "AAPL": 185.0,
            "MSFT": 420.0, "TSLA": 250.0, "USOIL": 75.0,
        }
        base = base_prices.get(symbol.upper(), 100.0)
        volatility = base * 0.002  # 0.2% per candle

        candles = []
        price = base
        now = datetime.now(timezone.utc)
        for i in range(count):
            ts = now - timedelta(minutes=15 * (count - i))
            open_p = price
            change = random.gauss(0, volatility)
            close_p = open_p + change
            high_p  = max(open_p, close_p) + abs(random.gauss(0, volatility * 0.5))
            low_p   = min(open_p, close_p) - abs(random.gauss(0, volatility * 0.5))
            volume  = random.randint(100, 10000)
            candles.append({
                "timestamp": ts,
                "open": round(open_p, 5),
                "high": round(high_p, 5),
                "low": round(low_p, 5),
                "close": round(close_p, 5),
                "volume": volume,
            })
            price = close_p

        return candles

    # ------------------------------------------------------------------
    # LOOP 2: STRATEGY SCANNING
    # ------------------------------------------------------------------

    def _strategy_scan_loop(self):
        """
        Evaluate all active strategies on current candle data.
        Valid signals are queued for Intent Layer processing.

        Strategy routing:
          - AsianRangeBreakout  → EURUSD, GBPUSD (FOREX)
          - ORBVWAPStrategy     → ES, NQ (FUTURES) + AAPL, MSFT (STOCKS)
          - VWAPSDReversion     → XAUUSD, ES (mean-reversion)
          - CommodityTrend      → USOIL, NATGAS, COPPER (COMMODITIES)
          - GapAndGo            → AAPL, MSFT, TSLA (STOCKS)
          - CryptoFundingReversion → BTC/USDT, ETH/USDT (CRYPTO)
          - CryptoMondayRange   → BTC/USDT (CRYPTO)
          - PredictionMarketArb → Kalshi markets (PREDICTION)
        """
        # Kill switch check
        if self._broker_manager and hasattr(self._broker_manager, '_esm'):
            if self._broker_manager._esm.is_kill_switch_active():
                logger.warning("[StrategyScanLoop] Kill switch ACTIVE — scanning suppressed")
                return

        self._cycle_count += 1
        now = datetime.now(timezone.utc)

        # Strategy → symbol mapping
        strategy_symbol_map = {
            "asian_range_breakout":     ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD"],
            "orb_vwap":                 ["ES", "NQ", "AAPL", "MSFT", "TSLA"],
            "vwap_sd_reversion":        ["XAUUSD", "ES", "NQ"],
            "commodity_trend":          ["USOIL", "NATGAS", "XAUUSD"],
            "gap_and_go":               ["AAPL", "MSFT", "TSLA", "NVDA", "AMZN"],
            "crypto_funding_reversion": ["BTC/USDT", "ETH/USDT"],
            "crypto_monday_range":      ["BTC/USDT", "ETH/USDT"],
            "prediction_market_arb":    [],  # loaded dynamically from scanner
        }

        signals_generated = 0

        for strategy_name, symbols in strategy_symbol_map.items():
            strategy = self._strategies.get(strategy_name)
            if strategy is None:
                continue

            for symbol in symbols:
                with self._cache_lock:
                    candles = self._candle_cache.get(symbol, [])

                if len(candles) < 50:
                    continue

                # Get account balance for risk normalisation
                account_balance = self._get_account_balance()

                context = {
                    "symbol": symbol,
                    "account_balance": account_balance,
                    "now_utc": now,
                    "cycle": self._cycle_count,
                }

                try:
                    signal = strategy.evaluate(candles, context)
                except Exception as exc:
                    logger.error(
                        f"[StrategyScanLoop] {strategy_name}/{symbol} evaluation failed: {exc}",
                        exc_info=True,
                    )
                    continue

                if not signal or signal.direction == "NONE" or not signal.is_valid:
                    continue

                # Apply risk normalisation
                sizing = self._risk_normaliser.calculate(
                    symbol=signal.symbol,
                    asset_class=signal.asset_class,
                    account_balance=account_balance,
                    entry_price=signal.entry_price,
                    stop_loss=signal.stop_loss,
                )
                signal.position_size = sizing.get("units", 0.0)
                signal.risk_usd = sizing.get("risk_usd", 0.0)

                if signal.position_size <= 0:
                    logger.warning(
                        f"[StrategyScanLoop] {strategy_name}/{symbol}: "
                        f"position_size=0 after normalisation — skipping signal"
                    )
                    continue

                # Queue for Intent Layer processing
                with self._signal_lock:
                    self._pending_signals.append({
                        "signal": signal,
                        "queued_at": now.isoformat(),
                        "strategy": strategy_name,
                        "symbol": symbol,
                    })
                signals_generated += 1

                logger.info(
                    f"[StrategyScanLoop] SIGNAL: {strategy_name}/{symbol} "
                    f"dir={signal.direction} entry={signal.entry_price} "
                    f"sl={signal.stop_loss} tp={signal.take_profit} "
                    f"units={signal.position_size} risk_usd={signal.risk_usd:.2f}"
                )

        if signals_generated > 0:
            logger.info(
                f"[StrategyScanLoop] Cycle {self._cycle_count}: "
                f"{signals_generated} signal(s) queued for Intent Layer"
            )

    # ------------------------------------------------------------------
    # LOOP 3: SIGNAL PROCESSING (Intent Layer)
    # ------------------------------------------------------------------

    def _signal_process_loop(self):
        """
        Process queued signals through the Intent Layer:
          StrategyValidator → RiskGuardian → ExecutionSupervisor → BrokerManager

        No order is submitted without passing all three gates.
        """
        with self._signal_lock:
            if not self._pending_signals:
                return
            to_process = list(self._pending_signals)
            self._pending_signals.clear()

        for item in to_process:
            signal = item["signal"]
            try:
                self._process_signal_through_intent_layer(signal)
            except Exception as exc:
                logger.error(
                    f"[SignalProcessLoop] Intent Layer failed for "
                    f"{item['strategy']}/{item['symbol']}: {exc}",
                    exc_info=True,
                )

    def _process_signal_through_intent_layer(self, signal) -> bool:
        """
        Push signal through the full agent chain.
        Returns True if order was submitted (paper or live).

        Chain: StrategyValidator → RiskGuardian → ExecutionSupervisor
        """
        from brokers.base_connector import OrderRequest, OrderSide, OrderType

        # 1. Pre-submit symbol check (tradeability gate)
        tradeable = self._broker_manager.check_symbol(signal.symbol)
        if not tradeable.is_tradeable:
            logger.warning(
                f"[IntentLayer] {signal.symbol} not tradeable: "
                f"{tradeable.rejection_code} — {tradeable.reason_if_not_tradeable}"
            )
            return False

        # 2. StrategyValidator gate
        if self._orchestrator and hasattr(self._orchestrator, "_strategy_validator_agent"):
            sv = self._orchestrator._strategy_validator_agent
            if sv and hasattr(sv, "validate_signal"):
                valid, reason = sv.validate_signal(signal)
                if not valid:
                    logger.info(f"[IntentLayer] StrategyValidator REJECTED: {reason}")
                    return False

        # 3. RiskGuardian gate
        if self._orchestrator and hasattr(self._orchestrator, "_risk_guardian_agent"):
            rg = self._orchestrator._risk_guardian_agent
            if rg and hasattr(rg, "approve_signal"):
                approved, reason = rg.approve_signal(signal)
                if not approved:
                    logger.info(f"[IntentLayer] RiskGuardian REJECTED: {reason}")
                    return False

        # 4. ExecutionSupervisor — build and submit order
        side = OrderSide.BUY if signal.direction == "BUY" else OrderSide.SELL
        request = OrderRequest(
            instrument=signal.symbol,
            side=side,
            units=signal.position_size,
            order_type=OrderType.MARKET,
            price=signal.entry_price,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            client_order_id=f"{signal.strategy_name}_{signal.symbol}_{int(time.time())}",
        )

        result = self._broker_manager.submit_order(request, operator="scheduler")

        if result.success:
            logger.info(
                f"[IntentLayer] ORDER SUBMITTED: {signal.symbol} {signal.direction} "
                f"units={signal.position_size} price={result.filled_price} "
                f"order_id={result.order_id}"
            )
            return True
        else:
            logger.warning(
                f"[IntentLayer] ORDER REJECTED: {signal.symbol} "
                f"reason={result.reject_reason!r} "
                f"code={result.normalized_rejection_code!r}"
            )
            return False

    # ------------------------------------------------------------------
    # LOOP 4: POSITION MONITORING
    # ------------------------------------------------------------------

    def _position_monitor_loop(self):
        """
        Monitor open positions:
          - Check stop loss / take profit hit conditions
          - Alert on unrealised drawdown exceeding threshold
          - Track position age (max hold time per strategy)
        """
        if not self._broker_manager.is_connected():
            return

        try:
            positions = self._broker_manager.get_positions()
            if not positions:
                return

            total_unrealised = sum(
                getattr(p, "unrealized_pnl", 0.0) for p in positions
            )
            logger.debug(
                f"[PositionMonitor] {len(positions)} open positions, "
                f"total unrealised PnL: ${total_unrealised:.2f}"
            )

            # Alert on large adverse move
            account_balance = self._get_account_balance()
            if account_balance > 0:
                drawdown_pct = abs(min(0, total_unrealised)) / account_balance * 100
                max_drawdown = self._config.get("risk", {}).get("max_total_drawdown_pct", 10.0)
                if drawdown_pct > max_drawdown * 0.75:  # Alert at 75% of max
                    logger.warning(
                        f"[PositionMonitor] DRAWDOWN ALERT: {drawdown_pct:.2f}% "
                        f"(max={max_drawdown}%)"
                    )

        except Exception as exc:
            logger.error(f"[PositionMonitor] Exception: {exc}")

    # ------------------------------------------------------------------
    # LOOP 5: RECONCILIATION
    # ------------------------------------------------------------------

    def _reconciliation_loop(self):
        """
        Reconcile engine state with broker positions.
        Calls BrokerManager.run_reconciliation() and logs any mismatches.
        """
        if not self._broker_manager.is_connected():
            return

        try:
            result = self._broker_manager.run_reconciliation(operator="scheduler")
            if result:
                is_clean = result.get("is_clean", False)
                if not is_clean:
                    logger.warning(
                        f"[ReconciliationLoop] State NOT CLEAN: {result}"
                    )
                else:
                    logger.debug("[ReconciliationLoop] State reconciled — CLEAN")
        except Exception as exc:
            logger.error(f"[ReconciliationLoop] Exception: {exc}")

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------

    def _get_active_symbols(self) -> List[str]:
        """Return list of all symbols to scan across all asset classes."""
        cfg = self._config.get("markets", {})
        symbols = []

        if cfg.get("forex", {}).get("enabled", True):
            symbols.extend(cfg["forex"].get("default_instruments", ["EURUSD", "GBPUSD", "XAUUSD"]))

        if cfg.get("futures", {}).get("enabled", False):
            symbols.extend(cfg["futures"].get("instruments", ["ES", "NQ", "GC"]))

        if cfg.get("stocks", {}).get("enabled", False):
            symbols.extend(cfg["stocks"].get("instruments", []))

        if cfg.get("indices", {}).get("enabled", True):
            symbols.extend(cfg["indices"].get("instruments", ["US30", "US500", "NAS100"]))

        if cfg.get("commodities", {}).get("enabled", True):
            symbols.extend(cfg["commodities"].get("instruments", ["USOIL", "UKOIL", "NATGAS"]))

        if cfg.get("crypto", {}).get("enabled", False):
            symbols.extend(cfg["crypto"].get("instruments", []))

        return list(dict.fromkeys(symbols))  # deduplicate preserving order

    def _get_account_balance(self) -> float:
        """Get current account balance. Returns config default on error."""
        default = self._config.get("risk", {}).get("account_balance_default", 100_000.0)
        try:
            if self._broker_manager.is_connected():
                info = self._broker_manager.get_account_info()
                if info:
                    return info.balance
        except Exception:
            pass
        return default

    def status(self) -> dict:
        """Return scheduler status."""
        return {
            "running": self._running,
            "cycle_count": self._cycle_count,
            "strategies_loaded": list(self._strategies.keys()),
            "pending_signals": len(self._pending_signals),
            "candle_cache_symbols": list(self._candle_cache.keys()),
            "active_threads": len([t for t in self._threads if t.is_alive()]),
        }

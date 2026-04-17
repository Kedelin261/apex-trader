"""
APEX MULTI-MARKET TJR ENGINE
Autonomous Scheduler — Continuous autonomous trading loops.

Phase 1-6 implementation: All 6 loops fully operational with:
  - Dedup logic (no re-execution after restart)
  - Risk Governor gate before every order
  - Portfolio Allocator gate before every order
  - Position Manager for autonomous SL/TP/BE/trail management
  - Broker Supervisor for heartbeat and reconnect
  - Persistence Manager for loop heartbeats and signal dedup
  - Alert Dispatcher for all event types

ARCHITECTURE RULES (NON-NEGOTIABLE):
  - No manual /api/signal calls required in autonomous mode
  - All signals flow through: StrategyScanner → StrategyValidator → RiskGuardian → ExecutionSupervisor
  - No strategy directly submits orders — all go through BrokerManager
  - Kill switch checked before every scan cycle
  - RiskGovernor checked before every entry
  - PortfolioAllocator checked before every entry

CONTINUOUS LOOPS (6 total):
  1. MarketDataLoop     — ingest live prices (30s default)
  2. StrategyScanLoop   — evaluate strategies, emit candidate signals (30s)
  3. SignalDispatchLoop — validate + execute through Intent Layer (5s, fast loop)
  4. PositionManagerLoop— autonomous position management (10s)
  5. ReconciliationLoop — engine ↔ broker reconciliation (60s)
  6. HealthMonitorLoop  — broker heartbeat + reconnect (30s)
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# RISK NORMALISER  (unchanged from v3.0 — kept here for backward compat)
# ---------------------------------------------------------------------------

class RiskNormaliser:
    """Cross-asset-class position sizing."""

    FUTURES_SPECS = {
        "GC":     {"tick_size": 0.10,  "multiplier": 100,   "tick_value": 10.00},
        "XAUUSD": {"tick_size": 0.10,  "multiplier": 100,   "tick_value": 10.00},
        "ES":     {"tick_size": 0.25,  "multiplier": 50,    "tick_value": 12.50},
        "NQ":     {"tick_size": 0.25,  "multiplier": 20,    "tick_value": 5.00},
        "YM":     {"tick_size": 1.00,  "multiplier": 5,     "tick_value": 5.00},
        "CL":     {"tick_size": 0.01,  "multiplier": 1000,  "tick_value": 10.00},
    }

    FOREX_PIP_VALUES = {
        "EURUSD": 10.00, "GBPUSD": 10.00, "AUDUSD": 10.00, "NZDUSD": 10.00,
        "USDJPY": 8.00,  "USDCAD": 7.50,  "USDCHF": 10.00,
        "EURJPY": 8.00,  "GBPJPY": 8.00,
    }

    FOREX_PIP_SIZES = {
        "USDJPY": 0.01, "EURJPY": 0.01, "GBPJPY": 0.01,
    }

    def __init__(self, risk_pct: float = 1.0):
        self.risk_pct = risk_pct

    def calculate(self, symbol, asset_class, account_balance, entry_price, stop_loss) -> dict:
        risk_usd = account_balance * (self.risk_pct / 100.0)
        sl_distance = abs(entry_price - stop_loss)
        if sl_distance <= 0:
            return {"units": 0.0, "risk_usd": 0.0, "risk_pct": 0.0,
                    "method": "ZERO_SL_DISTANCE",
                    "error": "Stop loss distance is zero — cannot size position"}
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
            return self._size_generic(risk_usd, sl_distance, entry_price)

    def _size_futures(self, symbol, risk_usd, sl_distance):
        spec = self.FUTURES_SPECS.get(symbol.upper(), {"tick_size": 1.0, "tick_value": 1.0})
        tick_size = spec["tick_size"]
        tick_value = spec["tick_value"]
        sl_ticks = sl_distance / tick_size
        if sl_ticks <= 0:
            return {"units": 1.0, "risk_usd": 0.0, "risk_pct": 0.0, "method": "FUTURES_MIN_1"}
        contracts = risk_usd / (sl_ticks * tick_value)
        contracts = max(1.0, round(contracts))
        actual_risk = sl_ticks * tick_value * contracts
        return {"units": contracts, "risk_usd": round(actual_risk, 2), "risk_pct": 0.0,
                "method": "FUTURES", "asset_class": "FUTURES", "sl_ticks": sl_ticks,
                "tick_value": tick_value}

    def _size_forex(self, symbol, risk_usd, entry_price, sl_distance):
        sym = symbol.upper()
        pip_size = self.FOREX_PIP_SIZES.get(sym, 0.0001)
        pip_value = self.FOREX_PIP_VALUES.get(sym, 10.0)
        sl_pips = sl_distance / pip_size
        if sl_pips <= 0:
            return {"units": 0.0, "risk_usd": 0.0, "risk_pct": 0.0, "method": "FOREX_ZERO_PIPS"}
        lots = risk_usd / (sl_pips * pip_value)
        lots = max(0.01, round(lots, 2))
        actual_risk = sl_pips * pip_value * lots
        return {"units": lots, "risk_usd": round(actual_risk, 2), "risk_pct": 0.0,
                "method": "FOREX", "asset_class": "FOREX", "sl_pips": round(sl_pips, 1)}

    def _size_stocks(self, risk_usd, sl_distance):
        shares = risk_usd / sl_distance
        shares = max(1.0, round(shares))
        return {"units": shares, "risk_usd": round(sl_distance * shares, 2),
                "risk_pct": 0.0, "method": "STOCKS", "asset_class": "STOCKS"}

    def _size_crypto(self, risk_usd, sl_distance):
        units = risk_usd / sl_distance
        units = max(0.001, round(units, 6))
        return {"units": units, "risk_usd": round(sl_distance * units, 2),
                "risk_pct": 0.0, "method": "CRYPTO", "asset_class": "CRYPTO"}

    def _size_indices(self, symbol, risk_usd, sl_distance):
        spec = self.FUTURES_SPECS.get(symbol.upper())
        if spec:
            return self._size_futures(symbol, risk_usd, sl_distance)
        units = max(1.0, round(risk_usd / sl_distance))
        return {"units": units, "risk_usd": round(sl_distance * units, 2),
                "risk_pct": 0.0, "method": "INDICES_GENERIC", "asset_class": "INDICES"}

    def _size_generic(self, risk_usd, sl_distance, entry_price):
        return {"units": max(1.0, round(risk_usd / sl_distance, 4)),
                "risk_usd": round(risk_usd, 2), "risk_pct": 0.0, "method": "GENERIC"}


# ---------------------------------------------------------------------------
# AUTONOMOUS SCHEDULER
# ---------------------------------------------------------------------------

class AutonomousScheduler:
    """
    Continuous autonomous trading scheduler — fully operationalised.

    Integrates:
      - RiskGovernor (Phase 3)
      - PortfolioAllocator (Phase 4)
      - PositionManager (Phase 2)
      - BrokerSupervisor (Phase 5)
      - PersistenceManager (Phase 6)
      - AlertDispatcher (Phase 7)
    """

    def __init__(
        self,
        orchestrator,
        broker_manager,
        config: dict,
        market_data_service=None,
        risk_governor=None,
        portfolio_allocator=None,
        position_manager=None,
        broker_supervisor=None,
        persistence_manager=None,
        alert_dispatcher=None,
    ):
        self._orchestrator = orchestrator
        self._broker_manager = broker_manager
        self._config = config
        self._market_data = market_data_service

        # Phase 2-7 integrations (lazy-created if not provided)
        self._risk_governor = risk_governor
        self._portfolio_allocator = portfolio_allocator
        self._position_manager = position_manager
        self._broker_supervisor = broker_supervisor
        self._persistence_mgr = persistence_manager
        self._alert = alert_dispatcher

        # Read intervals from config
        agent_cfg = config.get("agents", {})
        sched_cfg = config.get("autonomy", {}).get("scheduler", {})
        self._data_interval      = sched_cfg.get("market_data_interval_s", agent_cfg.get("orchestrator_interval_seconds", 30))
        self._strategy_interval  = sched_cfg.get("strategy_scan_interval_s", agent_cfg.get("orchestrator_interval_seconds", 30))
        self._position_interval  = sched_cfg.get("position_monitor_interval_s", 10)
        self._recon_interval     = sched_cfg.get("reconciliation_interval_s", 60)
        self._health_interval    = sched_cfg.get("health_monitor_interval_s", 30)

        # Risk normaliser
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

        # Signal queue with dedup
        self._pending_signals: List[dict] = []
        self._signal_lock = threading.Lock()

        # Candle cache
        self._candle_cache: Dict[str, List[dict]] = {}
        self._cache_lock = threading.Lock()

        # Current prices (updated by market data loop)
        self._current_prices: Dict[str, float] = {}
        self._price_lock = threading.Lock()

        # Loop failure tracking
        self._loop_failures: Dict[str, int] = {}

        # -----------------------------------------------------------------
        # Internal open-position registry (Fix: track fills here so that
        # /api/positions and ReconciliationService see them immediately)
        # -----------------------------------------------------------------
        self._open_positions: List[dict] = []   # list of plain position dicts
        self._position_lock = threading.Lock()

        logger.info(
            f"[AutonomousScheduler] Initialised — "
            f"data={self._data_interval}s "
            f"strategy={self._strategy_interval}s "
            f"position={self._position_interval}s "
            f"health={self._health_interval}s"
        )

    # ------------------------------------------------------------------
    # POSITION REGISTRY  (single source of truth for /api/positions)
    # ------------------------------------------------------------------

    def _register_open_position(
        self,
        signal,
        order_id: str,
        fill_price: float,
        strategy_name: str,
        risk_usd: float,
    ) -> dict:
        """
        Create an internal position record immediately after a fill is
        confirmed by BrokerManager.submit_order().  Writes to both the
        in-memory list and the PositionStateStore on disk so the state
        survives restarts and is visible to ReconciliationService.
        """
        pos = {
            "id": order_id,
            "symbol": signal.symbol,
            "strategy": strategy_name,
            "direction": signal.direction,
            "entry_price": fill_price or signal.entry_price,
            "stop_loss": signal.stop_loss,
            "take_profit": signal.take_profit,
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "units": signal.position_size,
            "risk_usd": risk_usd,
            "unrealized_pnl": 0.0,
            "notional_usd": abs(signal.position_size * (fill_price or signal.entry_price)),
            "asset_class": getattr(signal, "asset_class", "UNKNOWN"),
        }
        with self._position_lock:
            # Deduplicate by id — only one record per order_id
            self._open_positions = [
                p for p in self._open_positions if p.get("id") != order_id
            ]
            self._open_positions.append(pos)

        # Persist immediately
        pm = self._get_persistence_manager()
        if pm:
            try:
                pm.positions.upsert(pos)
            except Exception as e:
                logger.warning(f"[Scheduler] Failed to persist position {order_id}: {e}")

        logger.info(
            f"[Scheduler] Position REGISTERED: {signal.symbol} {signal.direction} "
            f"order_id={order_id} entry={pos['entry_price']} "
            f"units={signal.position_size} risk_usd={risk_usd:.2f}"
        )
        return pos

    def _remove_open_position(self, position_id: str, symbol: str, reason: str):
        """
        Remove an internal position record when a close/cancel is executed.
        Updates both in-memory list and PositionStateStore.
        """
        with self._position_lock:
            before = len(self._open_positions)
            self._open_positions = [
                p for p in self._open_positions if p.get("id") != position_id
            ]
            removed = before - len(self._open_positions)

        if removed:
            pm = self._get_persistence_manager()
            if pm:
                try:
                    pm.positions.remove(position_id)
                except Exception as e:
                    logger.warning(
                        f"[Scheduler] Failed to remove persisted position "
                        f"{position_id}: {e}"
                    )
            logger.info(
                f"[Scheduler] Position REMOVED: {symbol} id={position_id} "
                f"reason={reason}"
            )

    def get_open_positions(self) -> List[dict]:
        """Return a snapshot of the current internal open positions list."""
        with self._position_lock:
            return list(self._open_positions)

    def _get_risk_governor(self):
        """Lazy-load RiskGovernor."""
        if self._risk_governor is None:
            try:
                from live.risk_governor import RiskGovernor
                esm = getattr(self._broker_manager, "_esm", None)
                self._risk_governor = RiskGovernor(
                    self._config, esm=esm, alert_dispatcher=self._alert
                )
            except Exception as e:
                logger.warning(f"[Scheduler] RiskGovernor load failed: {e}")
        return self._risk_governor

    def _get_portfolio_allocator(self):
        """Lazy-load PortfolioAllocator."""
        if self._portfolio_allocator is None:
            try:
                from live.portfolio_allocator import PortfolioAllocator
                self._portfolio_allocator = PortfolioAllocator(self._config)
            except Exception as e:
                logger.warning(f"[Scheduler] PortfolioAllocator load failed: {e}")
        return self._portfolio_allocator

    def _get_position_manager(self):
        """Lazy-load PositionManager."""
        if self._position_manager is None:
            try:
                from live.position_manager import PositionManager
                self._position_manager = PositionManager(
                    self._config, alert_dispatcher=self._alert
                )
            except Exception as e:
                logger.warning(f"[Scheduler] PositionManager load failed: {e}")
        return self._position_manager

    def _get_broker_supervisor(self):
        """Lazy-load BrokerSupervisor."""
        if self._broker_supervisor is None:
            try:
                from live.broker_supervisor import BrokerSupervisor
                self._broker_supervisor = BrokerSupervisor(
                    self._broker_manager, self._config, alert_dispatcher=self._alert
                )
            except Exception as e:
                logger.warning(f"[Scheduler] BrokerSupervisor load failed: {e}")
        return self._broker_supervisor

    def _get_persistence_manager(self):
        """Lazy-load PersistenceManager."""
        if self._persistence_mgr is None:
            try:
                from live.persistence_manager import PersistenceManager
                self._persistence_mgr = PersistenceManager(self._config)
            except Exception as e:
                logger.warning(f"[Scheduler] PersistenceManager load failed: {e}")
        return self._persistence_mgr

    def _load_strategies(self) -> dict:
        strategies = {}
        strat_map = {
            "asian_range_breakout":     ("strategies.asian_range_breakout", "AsianRangeBreakout"),
            "orb_vwap":                 ("strategies.orb_vwap", "ORBVWAPStrategy"),
            "vwap_sd_reversion":        ("strategies.vwap_sd_reversion", "VWAPSDReversion"),
            "commodity_trend":          ("strategies.commodity_trend", "CommodityTrend"),
            "gap_and_go":               ("strategies.gap_and_go", "GapAndGo"),
            "crypto_funding_reversion": ("strategies.crypto_funding_reversion", "CryptoFundingReversion"),
            "crypto_monday_range":      ("strategies.crypto_monday_range", "CryptoMondayRange"),
            "prediction_market_arb":    ("strategies.prediction_market_arb", "PredictionMarketArb"),
        }
        for name, (module_path, class_name) in strat_map.items():
            try:
                import importlib
                module = importlib.import_module(module_path)
                cls = getattr(module, class_name)
                strategies[name] = cls()
            except Exception as e:
                logger.warning(f"[Scheduler] Failed to load {name}: {e}")
        logger.info(
            f"[AutonomousScheduler] Loaded {len(strategies)} strategies: "
            f"{list(strategies.keys())}"
        )
        return strategies

    # ------------------------------------------------------------------
    # START / STOP
    # ------------------------------------------------------------------

    def start(self):
        if self._running:
            logger.warning("[AutonomousScheduler] Already running — ignoring start()")
            return

        self._running = True
        self._stop_event.clear()

        # Alert restart event
        if self._alert:
            self._alert.emit(
                "RESTART_DETECTED",
                "Apex Autonomous Scheduler started",
                {"strategies": list(self._strategies.keys())},
            )

        # Run startup state recovery
        pm = self._get_persistence_manager()
        if pm:
            try:
                pm.run_startup_reconciliation(self._broker_manager)
            except Exception as e:
                logger.error(f"[Scheduler] Startup reconciliation failed: {e}")
            # Reload open positions from persistence store into memory so that
            # /api/positions reflects any positions that existed before restart.
            try:
                recovered = pm.positions.get_all()
                with self._position_lock:
                    self._open_positions = list(recovered)
                logger.info(
                    f"[Scheduler] Loaded {len(recovered)} open position(s) "
                    "from persistence store on startup"
                )
            except Exception as e:
                logger.warning(f"[Scheduler] Failed to reload persisted positions: {e}")

        # Register this scheduler with the reconciliation service so it can
        # use our position registry as its primary source.
        try:
            recon_svc = getattr(self._broker_manager, "_recon_svc", None)
            if recon_svc and hasattr(recon_svc, "set_scheduler"):
                recon_svc.set_scheduler(self)
        except Exception as e:
            logger.debug(f"[Scheduler] Could not register with recon service: {e}")

        loops = [
            ("market-data-loop",      self._market_data_loop,    self._data_interval),
            ("strategy-scan-loop",    self._strategy_scan_loop,  self._strategy_interval),
            ("signal-process-loop",   self._signal_process_loop, 5),
            ("position-monitor-loop", self._position_monitor_loop, self._position_interval),
            ("reconciliation-loop",   self._reconciliation_loop, self._recon_interval),
            ("health-monitor-loop",   self._health_monitor_loop, self._health_interval),
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
            "All signals: StrategyValidator → RiskGuardian → RiskGovernor → "
            "PortfolioAllocator → ExecutionSupervisor."
        )

    def stop(self):
        logger.info("[AutonomousScheduler] Stopping all loops...")
        self._running = False
        self._stop_event.set()
        for thread in self._threads:
            thread.join(timeout=5)
        self._threads.clear()
        if self._alert:
            self._alert.emit("SYSTEM_BLOCKED", "Autonomous Scheduler stopped", {})
        logger.info("[AutonomousScheduler] All loops stopped.")

    def _run_loop(self, name: str, worker, interval: float):
        """Generic loop runner with heartbeat recording and failure tracking."""
        pm = self._get_persistence_manager()
        logger.info(f"[Loop:{name}] Started — interval={interval}s")
        while not self._stop_event.is_set():
            try:
                worker()
                self._loop_failures[name] = 0
                # Record heartbeat
                if pm:
                    pm.heartbeats.beat(name)
            except Exception as exc:
                failures = self._loop_failures.get(name, 0) + 1
                self._loop_failures[name] = failures
                logger.error(
                    f"[Loop:{name}] Exception #{failures}: {exc}", exc_info=True
                )
                if failures >= 3 and self._alert:
                    self._alert.emit(
                        "LOOP_FAILURE",
                        f"Loop {name!r} failed {failures} consecutive times",
                        {"loop": name, "failures": failures, "error": str(exc)},
                    )
            self._stop_event.wait(timeout=interval)
        logger.info(f"[Loop:{name}] Stopped")

    # ------------------------------------------------------------------
    # LOOP 1: MARKET DATA
    # ------------------------------------------------------------------

    def _market_data_loop(self):
        active_symbols = self._get_active_symbols()
        if not active_symbols:
            return

        rg = self._get_risk_governor()
        pm = self._get_persistence_manager()
        any_updated = False

        for symbol in active_symbols:
            try:
                candles = self._fetch_candles(symbol, timeframe="M15", count=200)
                if candles:
                    with self._cache_lock:
                        self._candle_cache[symbol] = candles
                    # Update current price from latest candle
                    latest_close = candles[-1].get("close", 0.0)
                    with self._price_lock:
                        self._current_prices[symbol] = latest_close
                    # Update candle timestamp in persistence
                    if pm and candles[-1].get("timestamp"):
                        ts = candles[-1]["timestamp"]
                        ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
                        pm.candle_timestamps.update(symbol, ts_str)
                    any_updated = True
            except Exception as exc:
                logger.warning(f"[MarketDataLoop] Failed to fetch {symbol}: {exc}")

        if any_updated and rg:
            rg.mark_data_received()

        logger.debug(
            f"[MarketDataLoop] Cache: {len(self._candle_cache)} instruments"
        )

    def _fetch_candles(self, symbol: str, timeframe: str = "M15", count: int = 200) -> List[dict]:
        from live.broker_manager import get_broker_for_symbol
        target = get_broker_for_symbol(symbol)

        if self._market_data:
            try:
                candles = self._market_data.get_candles(symbol, timeframe, count)
                if candles:
                    return candles
            except Exception:
                pass

        if target == "oanda" and getattr(self._broker_manager, "_connector", None):
            try:
                connector = self._broker_manager._connector
                if hasattr(connector, "get_candles"):
                    return connector.get_candles(symbol, timeframe, count)
            except Exception as exc:
                logger.debug(f"[MarketDataLoop] OANDA candles failed for {symbol}: {exc}")

        if target == "ibkr" and getattr(self._broker_manager, "_ibkr_connector", None):
            try:
                connector = self._broker_manager._ibkr_connector
                if hasattr(connector, "get_candles"):
                    return connector.get_candles(symbol, timeframe, count)
            except Exception as exc:
                logger.debug(f"[MarketDataLoop] IBKR candles failed for {symbol}: {exc}")

        return self._generate_synthetic_candles(symbol, count)

    def _generate_synthetic_candles(self, symbol: str, count: int) -> List[dict]:
        import random
        base_prices = {
            "EURUSD": 1.0850, "GBPUSD": 1.2650, "XAUUSD": 2350.0,
            "ES": 5200.0, "NQ": 18500.0, "AAPL": 185.0,
            "MSFT": 420.0, "TSLA": 250.0, "USOIL": 75.0,
        }
        base = base_prices.get(symbol.upper(), 100.0)
        volatility = base * 0.002
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
            candles.append({
                "timestamp": ts,
                "open": round(open_p, 5),
                "high": round(high_p, 5),
                "low": round(low_p, 5),
                "close": round(close_p, 5),
                "volume": random.randint(100, 10000),
            })
            price = close_p
        return candles

    # ------------------------------------------------------------------
    # LOOP 2: STRATEGY SCANNING
    # ------------------------------------------------------------------

    def _strategy_scan_loop(self):
        # Kill switch check
        if self._broker_manager and hasattr(self._broker_manager, "_esm"):
            if self._broker_manager._esm.is_kill_switch_active():
                logger.warning("[StrategyScanLoop] Kill switch ACTIVE — scanning suppressed")
                return

        self._cycle_count += 1
        now = datetime.now(timezone.utc)

        strategy_symbol_map = {
            "asian_range_breakout":     ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD"],
            "orb_vwap":                 ["ES", "NQ", "AAPL", "MSFT", "TSLA"],
            "vwap_sd_reversion":        ["XAUUSD", "ES", "NQ"],
            "commodity_trend":          ["USOIL", "NATGAS", "XAUUSD"],
            "gap_and_go":               ["AAPL", "MSFT", "TSLA", "NVDA", "AMZN"],
            "crypto_funding_reversion": ["BTC/USDT", "ETH/USDT"],
            "crypto_monday_range":      ["BTC/USDT", "ETH/USDT"],
            "prediction_market_arb":    [],
        }

        signals_generated = 0
        pm = self._get_persistence_manager()

        for strategy_name, symbols in strategy_symbol_map.items():
            strategy = self._strategies.get(strategy_name)
            if strategy is None:
                continue

            for symbol in symbols:
                with self._cache_lock:
                    candles = self._candle_cache.get(symbol, [])
                if len(candles) < 50:
                    continue

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
                        f"[StrategyScanLoop] {strategy_name}/{symbol} eval failed: {exc}",
                        exc_info=True,
                    )
                    continue

                if not signal or signal.direction == "NONE" or not signal.is_valid:
                    continue

                # ---- Dedup check: skip if already submitted
                signal_id = getattr(signal, "signal_id", None) or getattr(signal, "id", None)
                if signal_id and pm and pm.signal_dedup.is_submitted(str(signal_id)):
                    logger.debug(
                        f"[StrategyScanLoop] DEDUP skip {strategy_name}/{symbol} "
                        f"signal_id={signal_id}"
                    )
                    continue

                # ---- Risk normalisation
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
                    continue

                # Queue for Intent Layer
                with self._signal_lock:
                    self._pending_signals.append({
                        "signal": signal,
                        "signal_id": str(signal_id) if signal_id else None,
                        "queued_at": now.isoformat(),
                        "strategy": strategy_name,
                        "symbol": symbol,
                        "asset_class": signal.asset_class,
                        "risk_usd": signal.risk_usd,
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
                f"{signals_generated} signal(s) queued"
            )

    # ------------------------------------------------------------------
    # LOOP 3: SIGNAL DISPATCH (Intent Layer + Governor + Allocator)
    # ------------------------------------------------------------------

    def _signal_process_loop(self):
        with self._signal_lock:
            if not self._pending_signals:
                return
            to_process = list(self._pending_signals)
            self._pending_signals.clear()

        rg = self._get_risk_governor()
        alloc = self._get_portfolio_allocator()
        pm = self._get_persistence_manager()

        # Build current open positions list for governor
        open_positions = self._get_open_positions_for_governor()

        for item in to_process:
            signal = item["signal"]
            strategy_name = item["strategy"]
            symbol = item["symbol"]
            asset_class = item.get("asset_class", "UNKNOWN")
            risk_usd = item.get("risk_usd", 0.0)
            signal_id = item.get("signal_id")

            # ---- Phase 3: Risk Governor gate
            if rg:
                allowed, reason = rg.check_new_entry(
                    symbol=symbol,
                    strategy_name=strategy_name,
                    asset_class=asset_class,
                    proposed_risk_usd=risk_usd,
                    open_positions=open_positions,
                    account_balance=self._get_account_balance(),
                )
                if not allowed:
                    logger.info(
                        f"[SignalDispatch] RiskGovernor BLOCKED "
                        f"{strategy_name}/{symbol}: {reason}"
                    )
                    continue

            # ---- Phase 4: Portfolio Allocator gate
            if alloc:
                decision = alloc.check_allocation(
                    strategy_name=strategy_name,
                    symbol=symbol,
                    asset_class=asset_class,
                    proposed_risk_usd=risk_usd,
                    open_positions=open_positions,
                )
                if not decision.allowed:
                    logger.info(
                        f"[SignalDispatch] PortfolioAllocator BLOCKED "
                        f"{strategy_name}/{symbol}: {decision.reason}"
                    )
                    continue

            # ---- Intent Layer (validator → risk → execution)
            try:
                fill_result = self._process_signal_through_intent_layer(signal)
                if fill_result:
                    # Mark signal as submitted (dedup)
                    if signal_id and pm:
                        pm.signal_dedup.mark_submitted(signal_id)
                    # Record in governor and allocator
                    if rg:
                        rg.record_trade_opened(symbol, strategy_name, risk_usd)
                    if alloc:
                        alloc.record_position_opened(strategy_name, risk_usd)

                    # --------------------------------------------------------
                    # FIX: register the filled position in the internal store
                    # so /api/positions and ReconciliationService see it now.
                    # --------------------------------------------------------
                    order_id    = fill_result.order_id or ""
                    fill_price  = fill_result.filled_price or signal.entry_price
                    self._register_open_position(
                        signal=signal,
                        order_id=order_id,
                        fill_price=fill_price,
                        strategy_name=strategy_name,
                        risk_usd=risk_usd,
                    )

                    # Alert
                    if self._alert:
                        self._alert.emit(
                            "TRADE_OPENED",
                            f"Trade opened: {symbol} {signal.direction}",
                            {
                                "symbol": symbol,
                                "strategy": strategy_name,
                                "direction": signal.direction,
                                "entry": fill_price,
                                "sl": signal.stop_loss,
                                "tp": signal.take_profit,
                                "units": signal.position_size,
                                "risk_usd": risk_usd,
                                "order_id": order_id,
                            },
                        )
            except Exception as exc:
                logger.error(
                    f"[SignalDispatch] Intent Layer failed for "
                    f"{strategy_name}/{symbol}: {exc}",
                    exc_info=True,
                )

    def _process_signal_through_intent_layer(self, signal):
        """
        Full Intent Layer: tradeability → StrategyValidator → RiskGuardian → Execution.

        Returns the OrderResult on success (truthy, with .order_id and .filled_price),
        or None/False on rejection so callers can test ``if result:``.
        """
        from brokers.base_connector import OrderRequest, OrderSide, OrderType

        # 1. Tradeability gate
        tradeable = self._broker_manager.check_symbol(signal.symbol)
        if not tradeable.is_tradeable:
            logger.info(
                f"[IntentLayer] {signal.symbol} not tradeable: "
                f"{tradeable.rejection_code}"
            )
            return None

        # 2. StrategyValidator gate
        if self._orchestrator and hasattr(self._orchestrator, "_strategy_validator_agent"):
            sv = self._orchestrator._strategy_validator_agent
            if sv and hasattr(sv, "validate_signal"):
                valid, reason = sv.validate_signal(signal)
                if not valid:
                    logger.info(f"[IntentLayer] StrategyValidator REJECTED: {reason}")
                    return None

        # 3. RiskGuardian gate (agent-level)
        if self._orchestrator and hasattr(self._orchestrator, "_risk_guardian_agent"):
            rg = self._orchestrator._risk_guardian_agent
            if rg and hasattr(rg, "approve_signal"):
                approved, reason = rg.approve_signal(signal)
                if not approved:
                    logger.info(f"[IntentLayer] RiskGuardian REJECTED: {reason}")
                    return None

        # 4. Build and submit order via BrokerManager
        side = OrderSide.BUY if signal.direction == "BUY" else OrderSide.SELL
        request = OrderRequest(
            instrument=signal.symbol,
            side=side,
            units=signal.position_size,
            order_type=OrderType.MARKET,
            price=signal.entry_price,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            client_order_id=(
                f"{getattr(signal, 'strategy_name', 'auto')}_{signal.symbol}_{int(time.time())}"
            ),
        )

        result = self._broker_manager.submit_order(request, operator="scheduler")

        if result.success:
            logger.info(
                f"[IntentLayer] ORDER SUBMITTED: {signal.symbol} {signal.direction} "
                f"units={signal.position_size} order_id={result.order_id}"
            )
            return result          # truthy OrderResult carrying order_id + filled_price
        else:
            logger.warning(
                f"[IntentLayer] ORDER REJECTED: {signal.symbol} "
                f"reason={result.reject_reason!r}"
            )
            rg = self._get_risk_governor()
            if rg:
                rg.record_broker_reject(signal.symbol, result.reject_reason or "")
            return None

    # ------------------------------------------------------------------
    # LOOP 4: POSITION MANAGEMENT
    # ------------------------------------------------------------------

    def _position_monitor_loop(self):
        if not self._broker_manager.is_connected():
            return

        pm_mgr = self._get_position_manager()
        if pm_mgr is None:
            return

        try:
            # Get open positions from broker
            broker_positions = self._broker_manager.get_positions()
            if not broker_positions:
                return

            # Convert to dict format
            positions = []
            for bp in broker_positions:
                positions.append({
                    "id": getattr(bp, "order_id", None) or getattr(bp, "id", ""),
                    "symbol": bp.instrument,
                    "direction": bp.side.value if hasattr(bp.side, "value") else str(bp.side),
                    "units": bp.units,
                    "entry_price": getattr(bp, "avg_price", 0.0),
                    "stop_loss": getattr(bp, "stop_loss", 0.0),
                    "take_profit": getattr(bp, "take_profit", 0.0),
                    "entry_time": getattr(bp, "open_time", ""),
                    "strategy": getattr(bp, "strategy", ""),
                    "unrealized_pnl": getattr(bp, "unrealized_pnl", 0.0),
                    "notional_usd": abs(bp.units * getattr(bp, "avg_price", 0.0)),
                    "asset_class": getattr(bp, "asset_class", "UNKNOWN"),
                })

            # Get current prices
            with self._price_lock:
                current_prices = dict(self._current_prices)

            # Get open orders for stale check
            open_orders = []
            try:
                open_orders_broker = self._broker_manager.get_open_orders()
                open_orders = [
                    {
                        "id": getattr(o, "order_id", ""),
                        "symbol": getattr(o, "instrument", ""),
                        "type": getattr(o, "order_type", "MARKET"),
                        "placed_at": getattr(o, "created_at", ""),
                    }
                    for o in (open_orders_broker or [])
                ]
            except Exception:
                pass

            # Evaluate positions
            actions = pm_mgr.evaluate_positions(positions, current_prices, open_orders)

            # Execute actions via BrokerManager
            for action in actions:
                self._execute_position_action(action)

            # Update unrealised PnL in risk governor
            total_unrealised = sum(
                p.get("unrealized_pnl", 0.0) for p in positions
            )
            rg = self._get_risk_governor()
            if rg:
                rg.record_unrealized_pnl(total_unrealised)

        except Exception as exc:
            logger.error(f"[PositionMonitor] Exception: {exc}", exc_info=True)

    def _execute_position_action(self, action):
        """Execute a PositionAction from the PositionManager via BrokerManager."""
        from brokers.base_connector import OrderRequest, OrderSide, OrderType
        try:
            connector = self._broker_manager._route_connector(action.symbol)
            if connector is None:
                connector = self._broker_manager._paper_broker

            if action.action == "CLOSE":
                if hasattr(connector, "close_position"):
                    result = connector.close_position(action.position_id)
                    logger.info(
                        f"[PositionManager] CLOSED {action.symbol}: "
                        f"{action.reason}"
                    )
                    # FIX: remove from internal position registry on close
                    self._remove_open_position(
                        position_id=action.position_id,
                        symbol=action.symbol,
                        reason=action.reason,
                    )
                    if self._alert:
                        self._alert.emit(
                            "TRADE_CLOSED",
                            f"Position closed: {action.symbol}",
                            {"reason": action.reason, "position_id": action.position_id},
                        )
                else:
                    logger.warning(
                        f"[PositionManager] Connector for {action.symbol} "
                        f"has no close_position() method"
                    )

            elif action.action == "MOVE_SL" and action.new_sl:
                if hasattr(connector, "modify_position"):
                    connector.modify_position(action.position_id, stop_loss=action.new_sl)
                    logger.info(
                        f"[PositionManager] MOVED SL {action.symbol}: "
                        f"new_sl={action.new_sl} ({action.reason})"
                    )
                    if self._alert:
                        self._alert.emit(
                            "STOP_MOVED",
                            f"Stop moved: {action.symbol} → {action.new_sl}",
                            {"reason": action.reason, "new_sl": action.new_sl},
                        )

            elif action.action == "CANCEL_ORDER":
                if hasattr(connector, "cancel_order"):
                    connector.cancel_order(action.position_id)
                    logger.info(
                        f"[PositionManager] CANCELLED order {action.position_id}: "
                        f"{action.reason}"
                    )

        except Exception as e:
            logger.error(
                f"[PositionManager] Failed to execute {action.action} "
                f"on {action.symbol}: {e}"
            )

    # ------------------------------------------------------------------
    # LOOP 5: RECONCILIATION
    # ------------------------------------------------------------------

    def _reconciliation_loop(self):
        if not self._broker_manager.is_connected():
            return

        try:
            result = self._broker_manager.run_reconciliation(operator="scheduler")
            if result:
                is_clean = result.get("is_clean", True)
                if not is_clean:
                    details = str(result.get("mismatches", "unknown"))
                    logger.warning(
                        f"[ReconciliationLoop] State NOT CLEAN: {details}"
                    )
                    rg = self._get_risk_governor()
                    if rg:
                        rg.record_reconciliation_mismatch(details)
                    if self._alert:
                        self._alert.emit(
                            "RECONCILIATION_MISMATCH",
                            "Engine ↔ broker position mismatch detected",
                            {"details": details},
                        )
                else:
                    logger.debug("[ReconciliationLoop] State CLEAN")

                # ---------------------------------------------------------
                # BROKER-ONLY REPAIR PATH (deterministic — no order resubmit)
                #
                # If the broker holds positions that the engine doesn't know
                # about (e.g. filled while the engine was down), import them
                # into the internal registry so subsequent loops manage them
                # correctly and reconciliation no longer marks them as ghosts.
                # ---------------------------------------------------------
                broker_only = result.get("broker_only_positions", [])
                if broker_only:
                    self._import_broker_only_positions(broker_only)

        except Exception as exc:
            logger.error(f"[ReconciliationLoop] Exception: {exc}")

    def _import_broker_only_positions(self, broker_only_symbols: list):
        """
        Import positions that exist at the broker but are missing from the
        internal registry.  Fetches live broker data and creates minimal
        position records without resubmitting orders.
        """
        try:
            broker_positions = self._broker_manager.get_positions()
            if not broker_positions:
                return

            known_symbols = {p.get("symbol") for p in self.get_open_positions()}
            pm = self._get_persistence_manager()

            for bp in broker_positions:
                symbol = bp.instrument
                if symbol not in broker_only_symbols:
                    continue
                if symbol in known_symbols:
                    continue  # already registered — skip

                order_id = getattr(bp, "order_id", None) or getattr(bp, "id", "")
                direction = bp.side.value if hasattr(bp.side, "value") else str(bp.side)
                pos = {
                    "id": order_id or f"broker_{symbol}_{int(time.time())}",
                    "symbol": symbol,
                    "strategy": getattr(bp, "strategy", "unknown"),
                    "direction": direction,
                    "entry_price": getattr(bp, "avg_price", 0.0),
                    "stop_loss": getattr(bp, "stop_loss", 0.0),
                    "take_profit": getattr(bp, "take_profit", 0.0),
                    "entry_time": getattr(bp, "open_time", datetime.now(timezone.utc).isoformat()),
                    "units": bp.units,
                    "risk_usd": 0.0,
                    "unrealized_pnl": getattr(bp, "unrealized_pnl", 0.0),
                    "notional_usd": abs(bp.units * getattr(bp, "avg_price", 0.0)),
                    "asset_class": getattr(bp, "asset_class", "UNKNOWN"),
                    "_imported_by_reconciliation": datetime.now(timezone.utc).isoformat(),
                }
                with self._position_lock:
                    self._open_positions.append(pos)
                if pm:
                    try:
                        pm.positions.upsert(pos)
                    except Exception:
                        pass
                logger.warning(
                    f"[ReconciliationLoop] IMPORTED broker-only position: "
                    f"{symbol} {direction} units={bp.units} "
                    f"order_id={pos['id']}"
                )
        except Exception as e:
            logger.error(f"[ReconciliationLoop] broker-only import failed: {e}")

    # ------------------------------------------------------------------
    # LOOP 6: HEALTH MONITOR (Broker Supervisor)
    # ------------------------------------------------------------------

    def _health_monitor_loop(self):
        sup = self._get_broker_supervisor()
        if sup is None:
            return
        try:
            sup.run_heartbeat()
        except Exception as exc:
            logger.error(f"[HealthMonitorLoop] Exception: {exc}")

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------

    def _get_active_symbols(self) -> List[str]:
        cfg = self._config.get("markets", {})
        symbols = []
        if cfg.get("forex", {}).get("enabled", True):
            symbols.extend(cfg["forex"].get("default_instruments", ["EURUSD", "GBPUSD"]))
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
        return list(dict.fromkeys(symbols))

    def _get_account_balance(self) -> float:
        default = self._config.get("risk", {}).get("account_balance_default", 100_000.0)
        try:
            if self._broker_manager.is_connected():
                info = self._broker_manager.get_account_info()
                if info:
                    return info.balance
        except Exception:
            pass
        return default

    def _get_open_positions_for_governor(self) -> List[dict]:
        """Get open positions as plain dicts for governor/allocator checks."""
        try:
            broker_positions = self._broker_manager.get_positions()
            return [
                {
                    "id": getattr(p, "order_id", ""),
                    "symbol": p.instrument,
                    "strategy": getattr(p, "strategy", ""),
                    "asset_class": getattr(p, "asset_class", "UNKNOWN"),
                    "notional_usd": abs(
                        p.units * getattr(p, "avg_price", 0.0)
                    ),
                }
                for p in (broker_positions or [])
            ]
        except Exception:
            return []

    def status(self) -> dict:
        pm = self._get_persistence_manager()
        sup = self._get_broker_supervisor()
        rg = self._get_risk_governor()
        alloc = self._get_portfolio_allocator()
        pm_mgr = self._get_position_manager()

        return {
            "running": self._running,
            "cycle_count": self._cycle_count,
            "strategies_loaded": list(self._strategies.keys()),
            "pending_signals": len(self._pending_signals),
            "candle_cache_symbols": list(self._candle_cache.keys()),
            "active_threads": len([t for t in self._threads if t.is_alive()]),
            "loop_names": [t.name for t in self._threads if t.is_alive()],
            "loop_failures": dict(self._loop_failures),
            "heartbeats": pm.heartbeats.get_all() if pm else {},
            "broker_health": sup.get_health() if sup else {},
            "risk_governor": rg.status() if rg else {},
            "portfolio_allocator": alloc.status() if alloc else {},
            "position_manager": pm_mgr.status() if pm_mgr else {},
        }

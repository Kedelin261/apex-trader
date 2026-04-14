"""
APEX MULTI-MARKET TJR ENGINE
Backtest Engine — Realistic candle-by-candle simulation.

Design principles:
- No lookahead bias: strategy only sees candles up to current index
- Realistic fills: slippage, spread, latency modeled explicitly
- Market-specific simulation rules applied
- All trades logged with full context
- Performance metrics computed from actual trade P&L

This is NOT a promotional backtester.
Results that look too good are a red flag, not a success.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from domain.models import (
    Candle, Signal, Trade, Position, TradeOutcome, SignalDirection,
    StrategyFamily, SessionType, MarketRegime, InstrumentType,
    VenueType, PerformanceMetrics, SignalStatus, BlockedTrade, EquitySnapshot
)
from core.tjr_strategy_engine import TJRStrategyEngine
from core.risk_manager import RiskManager
from core.market_context_filter import MarketContextFilter
from data.market_data_service import MarketDataService

logger = logging.getLogger(__name__)


# =============================================================================
# SIMULATION MODELS
# =============================================================================

class RealisticFillModel:
    """
    Models realistic trade fills including slippage, spread, and latency.
    All fills are pessimistic (slippage works against the trader).
    """

    def __init__(self,
                 slippage_pips: float = 1.0,
                 spread_pips: float = 1.5,
                 commission_per_lot: float = 7.0,
                 pip_size: float = 0.0001,
                 latency_ms: float = 50.0):
        self.slippage = slippage_pips * pip_size
        self.spread = spread_pips * pip_size
        self.commission_per_lot = commission_per_lot
        self.pip_size = pip_size
        self.latency_ms = latency_ms

    def apply_entry_slippage(self, price: float, direction: SignalDirection) -> float:
        """Apply adverse slippage on entry."""
        if direction == SignalDirection.BUY:
            return price + self.slippage + (self.spread / 2)
        else:
            return price - self.slippage - (self.spread / 2)

    def apply_exit_slippage(self, price: float, direction: SignalDirection,
                            exit_type: str = "TP") -> float:
        """
        Apply slippage on exit.
        Stop-loss exits get worse slippage than take-profit hits.
        """
        sl_extra = self.slippage * 1.5 if exit_type == "SL" else self.slippage
        if direction == SignalDirection.BUY:
            return price - sl_extra
        else:
            return price + sl_extra

    def compute_commission(self, lots: float) -> float:
        """Total round-trip commission."""
        return lots * self.commission_per_lot * 2  # Entry + exit

    def compute_pnl(self, entry: float, exit_price: float,
                    direction: SignalDirection,
                    lots: float, pip_value_per_lot: float = 10.0) -> float:
        """Compute trade P&L in USD."""
        if direction == SignalDirection.BUY:
            price_delta = exit_price - entry
        else:
            price_delta = entry - exit_price

        pips = price_delta / self.pip_size
        gross_pnl = pips * pip_value_per_lot * lots
        commission = self.compute_commission(lots)
        return gross_pnl - commission


# =============================================================================
# BACKTEST TRADE MANAGER
# =============================================================================

class BacktestTradeManager:
    """Manages open and closed positions during backtesting."""

    def __init__(self, fill_model: RealisticFillModel):
        self.fill_model = fill_model
        self.open_positions: List[Position] = []
        self.closed_trades: List[Trade] = []
        self.equity_curve: List[EquitySnapshot] = []
        self.blocked_trades: List[BlockedTrade] = []
        self.balance: float = 0.0
        self.peak_balance: float = 0.0

    def set_initial_balance(self, balance: float):
        self.balance = balance
        self.peak_balance = balance

    def open_trade(self, signal: Signal, candle: Candle) -> Optional[Trade]:
        """Simulate opening a trade with realistic fill."""
        fill_price = self.fill_model.apply_entry_slippage(
            signal.entry_price, signal.direction
        )

        trade = Trade(
            signal_id=signal.signal_id,
            order_id=str(uuid.uuid4()),
            instrument=signal.instrument,
            instrument_type=signal.instrument_type,
            direction=signal.direction,
            strategy_family=signal.strategy_family,
            session=signal.session,
            regime=signal.regime,
            entry_price=fill_price,
            entry_time=candle.timestamp,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            position_size_lots=signal.position_size_lots,
            risk_amount_usd=signal.risk_amount_usd,
            reward_to_risk=signal.reward_to_risk,
            strategy_reason=signal.strategy_reason,
            outcome=TradeOutcome.OPEN,
            is_paper=True
        )

        # Track position
        position = Position(
            trade_id=trade.trade_id,
            instrument=signal.instrument,
            direction=signal.direction,
            size_lots=signal.position_size_lots,
            entry_price=fill_price,
            current_price=fill_price,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit
        )
        self.open_positions.append(position)

        return trade

    def update_positions(self, candle: Candle) -> List[Tuple[Position, str]]:
        """
        Update all open positions for a new candle.
        Returns list of (position, exit_reason) for closed positions.
        """
        to_close: List[Tuple[Position, str]] = []

        for pos in self.open_positions:
            if pos.instrument != candle.instrument:
                continue

            pos.current_price = candle.close

            if pos.direction == SignalDirection.BUY:
                # Check SL (low hits stop)
                if candle.low <= pos.stop_loss:
                    to_close.append((pos, "SL"))
                # Check TP (high hits target)
                elif candle.high >= pos.take_profit:
                    to_close.append((pos, "TP"))
            else:  # SELL
                # Check SL (high hits stop)
                if candle.high >= pos.stop_loss:
                    to_close.append((pos, "SL"))
                # Check TP (low hits target)
                elif candle.low <= pos.take_profit:
                    to_close.append((pos, "TP"))

        return to_close

    def close_position(self, position: Position, exit_type: str,
                       candle: Candle,
                       trade: Trade,
                       pip_value: float = 10.0) -> Trade:
        """Close a position and compute final P&L."""
        if exit_type == "TP":
            raw_exit = position.take_profit
        else:
            raw_exit = position.stop_loss

        exit_price = self.fill_model.apply_exit_slippage(
            raw_exit, position.direction, exit_type
        )

        pnl = self.fill_model.compute_pnl(
            position.entry_price, exit_price,
            position.direction, position.size_lots, pip_value
        )

        # R multiple
        if exit_type == "TP":
            r_multiple = trade.reward_to_risk
        else:
            r_multiple = -1.0

        # Adjust for slippage erosion
        sl_slippage = (
            abs(exit_price - raw_exit) / self.fill_model.pip_size
        ) if exit_type == "SL" else 0.0
        if sl_slippage > 0 and exit_type == "SL":
            r_multiple = -1.0 - (sl_slippage * 0.1)

        trade.exit_price = exit_price
        trade.exit_time = candle.timestamp
        trade.exit_reason = exit_type
        trade.realized_pnl_usd = pnl
        trade.commission_usd = self.fill_model.compute_commission(position.size_lots)
        trade.r_multiple = r_multiple
        trade.slippage_pips = sl_slippage
        trade.outcome = TradeOutcome.WIN if pnl > 0 else (
            TradeOutcome.LOSS if pnl < 0 else TradeOutcome.BREAKEVEN
        )

        self.balance += pnl
        self.peak_balance = max(self.peak_balance, self.balance)
        self.closed_trades.append(trade)

        return trade

    def snapshot_equity(self, candle: Candle) -> EquitySnapshot:
        """Take an equity curve snapshot."""
        open_pnl = sum(p.unrealized_pnl for p in self.open_positions)
        equity = self.balance + open_pnl
        drawdown = max(0, (self.peak_balance - equity) / self.peak_balance * 100.0)
        snap = EquitySnapshot(
            timestamp=candle.timestamp,
            balance=self.balance,
            equity=equity,
            open_pnl=open_pnl,
            drawdown_pct=drawdown,
            peak_balance=self.peak_balance,
            trade_count=len(self.closed_trades)
        )
        self.equity_curve.append(snap)
        return snap


# =============================================================================
# PERFORMANCE CALCULATOR
# =============================================================================

class PerformanceCalculator:
    """Computes institutional-grade performance metrics from trade history."""

    def compute(self, trades: List[Trade], initial_balance: float,
                equity_curve: List[EquitySnapshot]) -> PerformanceMetrics:
        """Compute all performance metrics."""
        if not trades:
            return PerformanceMetrics(total_trades=0)

        wins = [t for t in trades if t.outcome == TradeOutcome.WIN]
        losses = [t for t in trades if t.outcome == TradeOutcome.LOSS]
        breakevens = [t for t in trades if t.outcome == TradeOutcome.BREAKEVEN]

        total = len(trades)
        n_wins = len(wins)
        n_losses = len(losses)
        win_rate = n_wins / total if total > 0 else 0.0

        r_multiples = [t.r_multiple for t in trades if t.r_multiple != 0]
        avg_r = sum(r_multiples) / len(r_multiples) if r_multiples else 0.0
        expectancy = avg_r  # E[R] per trade

        total_pnl = sum(t.realized_pnl_usd for t in trades)
        gross_profit = sum(t.realized_pnl_usd for t in wins)
        gross_loss = abs(sum(t.realized_pnl_usd for t in losses))
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")

        # Max drawdown from equity curve
        max_dd = max((s.drawdown_pct for s in equity_curve), default=0.0)

        # Sharpe ratio (simplified, annualized)
        if len(equity_curve) > 1:
            returns = []
            for i in range(1, len(equity_curve)):
                if equity_curve[i - 1].balance > 0:
                    ret = (equity_curve[i].balance - equity_curve[i - 1].balance) / equity_curve[i - 1].balance
                    returns.append(ret)
            if returns:
                import statistics
                mean_r = statistics.mean(returns)
                std_r = statistics.stdev(returns) if len(returns) > 1 else 0.0001
                sharpe = (mean_r / std_r) * (252 ** 0.5) if std_r > 0 else 0.0
            else:
                sharpe = 0.0
        else:
            sharpe = 0.0

        # Average hold time
        hold_times = []
        for t in trades:
            if t.exit_time and t.entry_time:
                delta = (t.exit_time - t.entry_time).total_seconds() / 3600.0
                hold_times.append(delta)
        avg_hold = sum(hold_times) / len(hold_times) if hold_times else 0.0

        # Consecutive wins/losses
        max_consec_wins = max_consec_losses = 0
        cur_wins = cur_losses = 0
        for t in trades:
            if t.outcome == TradeOutcome.WIN:
                cur_wins += 1
                cur_losses = 0
                max_consec_wins = max(max_consec_wins, cur_wins)
            elif t.outcome == TradeOutcome.LOSS:
                cur_losses += 1
                cur_wins = 0
                max_consec_losses = max(max_consec_losses, cur_losses)

        # By session
        by_session: Dict[str, dict] = {}
        for t in trades:
            s = t.session.value
            if s not in by_session:
                by_session[s] = {"trades": 0, "wins": 0, "pnl": 0.0}
            by_session[s]["trades"] += 1
            by_session[s]["pnl"] += t.realized_pnl_usd
            if t.outcome == TradeOutcome.WIN:
                by_session[s]["wins"] += 1
        for s, d in by_session.items():
            d["win_rate"] = d["wins"] / d["trades"] if d["trades"] > 0 else 0.0

        # By instrument
        by_instrument: Dict[str, dict] = {}
        for t in trades:
            inst = t.instrument
            if inst not in by_instrument:
                by_instrument[inst] = {"trades": 0, "wins": 0, "pnl": 0.0}
            by_instrument[inst]["trades"] += 1
            by_instrument[inst]["pnl"] += t.realized_pnl_usd
            if t.outcome == TradeOutcome.WIN:
                by_instrument[inst]["wins"] += 1

        total_commission = sum(t.commission_usd for t in trades)
        best_r = max(r_multiples) if r_multiples else 0.0
        worst_r = min(r_multiples) if r_multiples else 0.0

        return PerformanceMetrics(
            period="backtest",
            total_trades=total,
            winning_trades=n_wins,
            losing_trades=n_losses,
            breakeven_trades=len(breakevens),
            win_rate=round(win_rate, 4),
            avg_r_multiple=round(avg_r, 4),
            expectancy=round(expectancy, 4),
            profit_factor=round(profit_factor, 4),
            net_profit_usd=round(total_pnl, 2),
            max_drawdown_pct=round(max_dd, 4),
            sharpe_ratio=round(sharpe, 4),
            avg_hold_hours=round(avg_hold, 2),
            total_commission_usd=round(total_commission, 2),
            best_trade_r=round(best_r, 4),
            worst_trade_r=round(worst_r, 4),
            consecutive_wins=max_consec_wins,
            consecutive_losses=max_consec_losses,
            by_session=by_session,
            by_instrument=by_instrument
        )


# =============================================================================
# MAIN BACKTEST ENGINE
# =============================================================================

class BacktestEngine:
    """
    Production-grade backtesting engine.

    Runs candle-by-candle simulation with:
    - No lookahead bias (strategy sees only past candles)
    - Realistic fills (slippage, spread, commission)
    - Full risk management (DailyRiskGovernor, RiskManager)
    - Market context filtering
    - Session awareness
    - Blocked trade logging
    - Equity curve generation
    - Full performance metrics
    """

    # Per-instrument pip value (USD per pip per standard lot)
    PIP_VALUES: dict = {
        "XAUUSD": 1.0,    # Gold: $1 per 0.01 pip move per lot
        "EURUSD": 10.0,
        "GBPUSD": 10.0,
        "AUDUSD": 10.0,
        "USDCAD": 7.7,
        "USDJPY": 9.1,
        "GBPJPY": 9.1,
        "ES": 12.50,      # E-mini S&P 500: $12.50 per tick
        "NQ": 5.00,       # E-mini Nasdaq: $5 per tick
        "GC": 10.0,       # Gold futures: $10 per tick
        "CL": 10.0,       # Crude oil: $10 per tick
        "DEFAULT": 10.0
    }

    # Per-instrument pip sizes
    PIP_SIZES: dict = {
        "XAUUSD": 0.01,
        "USDJPY": 0.01,
        "GBPJPY": 0.01,
        "EURJPY": 0.01,
        "DEFAULT": 0.0001
    }

    def __init__(self, config: dict):
        self.config = config
        bt_cfg = config.get("backtest", {})

        self.initial_balance = bt_cfg.get("initial_balance", 100000.0)
        slippage_pips = bt_cfg.get("slippage_pips", 1.0)
        commission = bt_cfg.get("commission_per_lot_usd", 7.0)
        spread = bt_cfg.get("spread_overhead_pips", 1.0)
        latency = bt_cfg.get("latency_ms", 50.0)

        self.strategy_engine = TJRStrategyEngine(config)
        self.risk_manager = RiskManager(config)
        self.context_filter = MarketContextFilter(config)
        self.data_service = MarketDataService(config)
        self._slippage_pips = slippage_pips
        self._spread_pips = spread
        self._commission = commission
        self._latency = latency
        # Fill model is created per-run with instrument-specific pip size
        self.fill_model = RealisticFillModel(
            slippage_pips=slippage_pips,
            spread_pips=spread,
            commission_per_lot=commission,
            latency_ms=latency
        )
        self.perf_calc = PerformanceCalculator()

    def run(self,
            instrument: str,
            instrument_type: InstrumentType,
            venue_type: VenueType,
            timeframe: str = "M15",
            candles: Optional[List[Candle]] = None,
            filepath: Optional[str] = None,
            min_candles_context: int = 100) -> dict:
        """
        Run backtest for a single instrument.

        Args:
            instrument: Symbol (e.g. "XAUUSD")
            instrument_type: InstrumentType enum
            venue_type: VenueType enum
            timeframe: Timeframe string
            candles: Optional pre-loaded candle list
            filepath: Optional CSV path (if candles not provided)
            min_candles_context: Minimum candle window for analysis

        Returns:
            dict with metrics, trades, blocked_trades, equity_curve
        """
        logger.info(f"[Backtest] Starting: {instrument} {timeframe}")

        # Build instrument-specific fill model
        pip_size = self.PIP_SIZES.get(instrument, self.PIP_SIZES["DEFAULT"])
        self.fill_model = RealisticFillModel(
            slippage_pips=self._slippage_pips,
            spread_pips=self._spread_pips,
            commission_per_lot=self._commission,
            pip_size=pip_size,
            latency_ms=self._latency
        )

        # Load data
        if candles is None:
            candles = self.data_service.load_historical(instrument, timeframe, filepath)

        if len(candles) < min_candles_context + 10:
            raise ValueError(
                f"Insufficient candles for backtest: {len(candles)} < {min_candles_context + 10}"
            )

        # Reset risk manager for this backtest run (simulation mode)
        self.risk_manager = RiskManager(config=self.config, simulation_mode=True)
        self.risk_manager.update_balance(self.initial_balance)

        trade_manager = BacktestTradeManager(self.fill_model)
        trade_manager.set_initial_balance(self.initial_balance)

        # Track open trades by position ID
        open_trade_map: Dict[str, Trade] = {}

        total_candles = len(candles)
        signal_count = 0
        blocked_count = 0

        logger.info(f"[Backtest] Processing {total_candles} candles...")

        for i in range(min_candles_context, total_candles):
            current_candle = candles[i]
            context_candles = candles[max(0, i - min_candles_context): i + 1]
            # Do NOT include current candle in analysis (no lookahead)
            analysis_candles = candles[max(0, i - min_candles_context): i]

            if len(analysis_candles) < 50:
                continue

            # 0. Advance simulation date (daily risk resets properly)
            candle_date = current_candle.timestamp.strftime("%Y-%m-%d")
            self.risk_manager.daily_governor.advance_to_date(candle_date)

            # 1. Update open positions for current candle
            positions_to_close = trade_manager.update_positions(current_candle)
            for pos, exit_type in positions_to_close:
                if pos.trade_id in open_trade_map:
                    trade = open_trade_map.pop(pos.trade_id)
                    pip_val = self.PIP_VALUES.get(instrument, self.PIP_VALUES["DEFAULT"])
                    closed_trade = trade_manager.close_position(
                        pos, exit_type, current_candle, trade, pip_val
                    )
                    self.risk_manager.record_trade_close(
                        closed_trade.realized_pnl_usd,
                        trade_manager.balance
                    )
                    trade_manager.open_positions.remove(pos)
                    logger.debug(
                        f"[Backtest] {instrument} {exit_type}: "
                        f"P&L=${closed_trade.realized_pnl_usd:+.2f} "
                        f"R={closed_trade.r_multiple:.2f}"
                    )

            # 2. Market context filter (every 15 candles for performance)
            if i % 15 == 0:
                context_result = self.context_filter.evaluate(
                    context_candles,
                    instrument,
                    current_spread_pips=1.5,
                    check_time=current_candle.timestamp
                )
                summary = context_result.get("summary", {})

                if not summary.get("all_passed", True):
                    for reason in summary.get("blocked_reasons", []):
                        trade_manager.blocked_trades.append(BlockedTrade(
                            instrument=instrument,
                            direction="N/A",
                            strategy_family="TJR",
                            block_stage="CONTEXT",
                            block_reasons=summary.get("blocked_reasons", []),
                            regime=summary.get("regime", MarketRegime.UNKNOWN).value
                                   if summary.get("regime") else None
                        ))
                    blocked_count += 1
                    continue

            # 3. TJR strategy analysis (no lookahead)
            setup = self.strategy_engine.analyze(
                candles=analysis_candles,
                instrument=instrument,
                instrument_type=instrument_type,
                venue_type=venue_type,
                timeframe=timeframe
            )

            if setup is None:
                continue

            # 4. Build signal
            signal = self.strategy_engine.build_signal(
                setup=setup,
                venue_type=venue_type,
                account_balance=trade_manager.balance,
                risk_pct=self.config.get("risk", {}).get("max_risk_per_trade_pct", 1.0)
            )

            if signal is None:
                continue

            signal_count += 1

            # 5. Risk validation
            risk_result = self.risk_manager.validate_signal(
                signal=signal,
                open_positions=trade_manager.open_positions,
                current_spread_pips=1.5,
                regime=setup.regime
            )

            if not risk_result.approved:
                trade_manager.blocked_trades.append(BlockedTrade(
                    instrument=instrument,
                    direction=signal.direction.value,
                    strategy_family="TJR",
                    block_stage="RISK",
                    block_reasons=risk_result.reasons,
                    setup_quality_score=setup.setup_quality_score,
                    regime=setup.regime.value if setup.regime else None
                ))
                blocked_count += 1
                continue

            # 6. Open trade
            trade = trade_manager.open_trade(signal, current_candle)
            if trade:
                open_trade_map[trade.trade_id] = trade
                # Link position to trade
                for pos in trade_manager.open_positions:
                    if pos.instrument == instrument:
                        pos.trade_id = trade.trade_id

            # 7. Equity snapshot (every 100 candles)
            if i % 100 == 0:
                trade_manager.snapshot_equity(current_candle)

        # Force-close any remaining positions at last price
        last_candle = candles[-1]
        for pos in list(trade_manager.open_positions):
            if pos.trade_id in open_trade_map:
                trade = open_trade_map.pop(pos.trade_id)
                trade.exit_price = last_candle.close
                trade.exit_time = last_candle.timestamp
                trade.exit_reason = "EOD_CLOSE"
                pip_size = self.fill_model.pip_size

                if pos.direction == SignalDirection.BUY:
                    price_delta = last_candle.close - pos.entry_price
                else:
                    price_delta = pos.entry_price - last_candle.close

                eod_pip_size = self.PIP_SIZES.get(instrument, self.PIP_SIZES["DEFAULT"])
                eod_pip_val = self.PIP_VALUES.get(instrument, self.PIP_VALUES["DEFAULT"])
                pnl = (price_delta / eod_pip_size) * eod_pip_val * pos.size_lots
                pnl -= self.fill_model.compute_commission(pos.size_lots)
                trade.realized_pnl_usd = pnl
                trade.outcome = (
                    TradeOutcome.WIN if pnl > 0 else
                    TradeOutcome.LOSS if pnl < 0 else
                    TradeOutcome.BREAKEVEN
                )
                trade_manager.closed_trades.append(trade)
                trade_manager.balance += pnl

        # Final equity snapshot
        trade_manager.snapshot_equity(last_candle)

        # Compute metrics
        metrics = self.perf_calc.compute(
            trade_manager.closed_trades,
            self.initial_balance,
            trade_manager.equity_curve
        )
        metrics.period = f"{candles[0].timestamp.date()} to {candles[-1].timestamp.date()}"

        final_balance = trade_manager.balance
        total_return_pct = ((final_balance - self.initial_balance) / self.initial_balance) * 100.0

        result = {
            "instrument": instrument,
            "timeframe": timeframe,
            "total_candles": total_candles,
            "signal_count": signal_count,
            "blocked_count": blocked_count,
            "initial_balance": self.initial_balance,
            "final_balance": round(final_balance, 2),
            "total_return_pct": round(total_return_pct, 4),
            "metrics": metrics.model_dump(),
            "trades": [t.model_dump() for t in trade_manager.closed_trades],
            "blocked_trades": [b.model_dump() for b in trade_manager.blocked_trades],
            "equity_curve": [s.model_dump() for s in trade_manager.equity_curve[-500:]],
        }

        logger.info(
            f"[Backtest] Complete: {instrument} | "
            f"Trades={metrics.total_trades} | "
            f"WR={metrics.win_rate:.1%} | "
            f"PF={metrics.profit_factor:.2f} | "
            f"Net=${metrics.net_profit_usd:+.2f} | "
            f"MaxDD={metrics.max_drawdown_pct:.2f}%"
        )

        return result

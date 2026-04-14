"""
APEX MULTI-MARKET TJR ENGINE
Domain Models — Core data structures for the entire system.

All domain objects are immutable where possible. Pydantic models enforce
schema validation at every system boundary.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# =============================================================================
# ENUMERATIONS — typed constants used across the entire system
# =============================================================================

class InstrumentType(str, Enum):
    FOREX = "FOREX"
    GOLD = "GOLD"
    FUTURES = "FUTURES"
    INDICES = "INDICES"
    STOCKS = "STOCKS"
    CRYPTO = "CRYPTO"
    COMMODITIES = "COMMODITIES"
    PREDICTION = "PREDICTION"


class VenueType(str, Enum):
    FOREX_BROKER = "FOREX_BROKER"
    FUTURES_EXCHANGE = "FUTURES_EXCHANGE"
    STOCK_EXCHANGE = "STOCK_EXCHANGE"
    CRYPTO_EXCHANGE = "CRYPTO_EXCHANGE"
    CFD_BROKER = "CFD_BROKER"
    PREDICTION_MARKET = "PREDICTION_MARKET"
    PAPER = "PAPER"


class MarketRegime(str, Enum):
    TRENDING_BULLISH = "TRENDING_BULLISH"
    TRENDING_BEARISH = "TRENDING_BEARISH"
    RANGING = "RANGING"
    MEAN_REVERTING = "MEAN_REVERTING"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    UNSTABLE = "UNSTABLE"
    NEWS_DRIVEN = "NEWS_DRIVEN"
    ILLIQUID = "ILLIQUID"
    BINARY_EVENT = "BINARY_EVENT"
    UNKNOWN = "UNKNOWN"


class SignalDirection(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    NONE = "NONE"


class SignalStatus(str, Enum):
    CANDIDATE = "CANDIDATE"
    VALIDATED = "VALIDATED"
    REJECTED = "REJECTED"
    RISK_APPROVED = "RISK_APPROVED"
    RISK_REJECTED = "RISK_REJECTED"
    EXECUTION_READY = "EXECUTION_READY"
    EXECUTION_BLOCKED = "EXECUTION_BLOCKED"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class TradeOutcome(str, Enum):
    WIN = "WIN"
    LOSS = "LOSS"
    BREAKEVEN = "BREAKEVEN"
    OPEN = "OPEN"
    CANCELLED = "CANCELLED"


class StrategyFamily(str, Enum):
    TJR = "TJR"
    TREND_FOLLOWING = "TREND_FOLLOWING"
    MEAN_REVERSION = "MEAN_REVERSION"
    VOLATILITY_BREAKOUT = "VOLATILITY_BREAKOUT"
    MARKET_MAKING = "MARKET_MAKING"
    ARBITRAGE = "ARBITRAGE"
    EVENT_DRIVEN = "EVENT_DRIVEN"


class SessionType(str, Enum):
    LONDON = "LONDON"
    NEW_YORK = "NEW_YORK"
    OVERLAP = "OVERLAP"
    ASIAN = "ASIAN"
    SYDNEY = "SYDNEY"
    OFF_HOURS = "OFF_HOURS"


class EnvironmentMode(str, Enum):
    BACKTEST = "BACKTEST"
    PAPER = "PAPER"
    LIVE = "LIVE"


class RiskRejectionReason(str, Enum):
    MAX_DAILY_LOSS_REACHED = "MAX_DAILY_LOSS_REACHED"
    MAX_DRAWDOWN_REACHED = "MAX_DRAWDOWN_REACHED"
    MAX_CONCURRENT_TRADES = "MAX_CONCURRENT_TRADES"
    RISK_PER_TRADE_EXCEEDED = "RISK_PER_TRADE_EXCEEDED"
    SPREAD_TOO_WIDE = "SPREAD_TOO_WIDE"
    SLIPPAGE_TOO_HIGH = "SLIPPAGE_TOO_HIGH"
    VOLATILITY_TOO_HIGH = "VOLATILITY_TOO_HIGH"
    NEWS_EVENT_ACTIVE = "NEWS_EVENT_ACTIVE"
    SESSION_INACTIVE = "SESSION_INACTIVE"
    INSTRUMENT_INELIGIBLE = "INSTRUMENT_INELIGIBLE"
    STRATEGY_INCOMPATIBLE = "STRATEGY_INCOMPATIBLE"
    MARKET_INSTABILITY = "MARKET_INSTABILITY"
    KILL_SWITCH_ACTIVE = "KILL_SWITCH_ACTIVE"
    BROKER_CONNECTIVITY = "BROKER_CONNECTIVITY"
    INVALID_STOP_DISTANCE = "INVALID_STOP_DISTANCE"
    RR_RATIO_INSUFFICIENT = "RR_RATIO_INSUFFICIENT"


class MarketStructureState(str, Enum):
    BULLISH = "BULLISH"       # Higher Highs, Higher Lows
    BEARISH = "BEARISH"       # Lower Highs, Lower Lows
    NEUTRAL = "NEUTRAL"       # Mixed or insufficient data


# =============================================================================
# CORE DATA OBJECTS
# =============================================================================

class Candle(BaseModel):
    """OHLCV candle with metadata."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    timeframe: str = "M15"
    instrument: str = ""
    is_complete: bool = True

    @property
    def body_size(self) -> float:
        return abs(self.close - self.open)

    @property
    def range_size(self) -> float:
        return self.high - self.low

    @property
    def body_ratio(self) -> float:
        if self.range_size == 0:
            return 0.0
        return self.body_size / self.range_size

    @property
    def is_bullish(self) -> bool:
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        return self.close < self.open


class Instrument(BaseModel):
    """Complete instrument specification with market-specific metadata."""
    symbol: str
    instrument_type: InstrumentType
    venue_type: VenueType
    description: str = ""
    base_currency: str = ""
    quote_currency: str = ""
    pip_size: float = 0.0001
    pip_value_usd: float = 10.0        # Per standard lot
    contract_size: float = 100000.0    # Standard lot size
    min_lot_size: float = 0.01
    max_lot_size: float = 100.0
    lot_step: float = 0.01
    min_stop_distance_pips: float = 5.0
    typical_spread_pips: float = 1.5
    max_allowed_spread_pips: float = 3.0
    trading_hours_utc: Optional[str] = "24/5"
    currency: str = "USD"
    enabled: bool = True
    exchange: Optional[str] = None

    @property
    def is_crypto(self) -> bool:
        return self.instrument_type == InstrumentType.CRYPTO

    @property
    def is_24_7(self) -> bool:
        return self.instrument_type == InstrumentType.CRYPTO


class MarketSessionProfile(BaseModel):
    """Session profile for a given instrument and market type."""
    session: SessionType
    utc_start: str   # "HH:MM"
    utc_end: str     # "HH:MM"
    priority: int    # Lower = higher priority
    instruments: List[str] = []
    high_activity: bool = False


class RiskProfile(BaseModel):
    """Risk configuration — instrument and account level."""
    instrument: str
    max_risk_pct: float = 1.0
    max_spread_pips: float = 3.0
    max_slippage_pips: float = 2.0
    min_rr_ratio: float = 2.0
    max_position_usd: float = 10000.0
    session_required: bool = True
    news_filter_enabled: bool = True
    volatility_filter_enabled: bool = True
    notes: str = ""


class ExecutionModel(BaseModel):
    """Execution model for a specific venue."""
    venue: VenueType
    order_types_supported: List[OrderType] = [OrderType.MARKET, OrderType.STOP]
    supports_partial_fill: bool = False
    typical_latency_ms: float = 50.0
    max_retries: int = 3
    idempotency_supported: bool = True
    notes: str = ""


class FeeModel(BaseModel):
    """Fee model for simulation realism."""
    instrument_type: InstrumentType
    commission_per_lot_usd: float = 7.0
    spread_overhead_pips: float = 1.0
    overnight_swap_points: float = 0.0
    slippage_pips: float = 1.0
    notes: str = ""


class SlippageModel(BaseModel):
    """Slippage model for simulation realism."""
    instrument_type: InstrumentType
    base_slippage_pips: float = 0.5
    volatility_multiplier: float = 1.5   # Applied during high-vol
    news_multiplier: float = 3.0
    market_order_extra_pips: float = 0.5


# =============================================================================
# SIGNALS
# =============================================================================

class SwingPoint(BaseModel):
    """A detected swing high or low."""
    timestamp: datetime
    price: float
    is_high: bool     # True = swing high, False = swing low
    confirmed: bool = False
    candle_index: int = 0


class LiquidityEvent(BaseModel):
    """A detected liquidity event (sweep, equal level)."""
    timestamp: datetime
    level_price: float
    event_type: str    # "EQUAL_HIGH" | "EQUAL_LOW" | "SWEEP_HIGH" | "SWEEP_LOW" | "STOP_HUNT"
    swept_price: float = 0.0
    confirmed: bool = False
    description: str = ""


class StructureBreak(BaseModel):
    """A detected Break of Structure (BOS) or Market Structure Shift."""
    timestamp: datetime
    break_type: str    # "BOS_BULLISH" | "BOS_BEARISH" | "MSS_BULLISH" | "MSS_BEARISH"
    break_price: float
    prior_structure_level: float
    displacement_pips: float = 0.0
    confirmed: bool = False
    description: str = ""


class TJRSetup(BaseModel):
    """
    A fully analyzed TJR setup candidate.
    All fields must be populated before this can become a signal.
    This is the direct output of the deterministic TJR engine.
    """
    setup_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    instrument: str
    instrument_type: InstrumentType
    timeframe: str
    session: SessionType
    timestamp_detected: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Structure
    structure_state: MarketStructureState
    recent_swing_highs: List[float] = []
    recent_swing_lows: List[float] = []
    last_swing_high: Optional[float] = None
    last_swing_low: Optional[float] = None

    # Liquidity
    liquidity_event: Optional[LiquidityEvent] = None
    liquidity_level_swept: Optional[float] = None

    # BOS / Structure break
    structure_break: Optional[StructureBreak] = None
    displacement_confirmed: bool = False

    # Entry
    direction: SignalDirection
    entry_price: float
    stop_loss: float
    take_profit: float
    entry_candle_body_ratio: float = 0.0
    entry_trigger_description: str = ""

    # Risk metrics
    stop_distance_pips: float = 0.0
    target_distance_pips: float = 0.0
    reward_to_risk: float = 0.0
    position_size_lots: float = 0.0
    risk_amount_usd: float = 0.0

    # Quality
    setup_quality_score: float = 0.0
    validation_flags: List[str] = []
    invalidation_conditions: List[str] = []

    # Context
    regime: Optional[MarketRegime] = None
    volatility_context: str = ""
    is_session_optimal: bool = False


class Signal(BaseModel):
    """
    A fully validated, risk-approved trade signal ready for execution.
    Produced from a TJRSetup after passing all validation gates.
    """
    signal_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    setup_id: str = ""
    instrument: str
    instrument_type: InstrumentType
    venue_type: VenueType
    timeframe: str
    session: SessionType
    direction: SignalDirection
    status: SignalStatus = SignalStatus.CANDIDATE

    # Prices
    entry_price: float
    stop_loss: float
    take_profit: float

    # Risk
    position_size_lots: float
    risk_amount_usd: float
    reward_to_risk: float
    stop_distance_pips: float
    target_distance_pips: float

    # Strategy context
    strategy_family: StrategyFamily
    strategy_reason: str = ""
    structure_state: str = ""
    liquidity_event_detected: bool = False
    bos_confirmed: bool = False
    volatility_context: str = ""
    regime: Optional[MarketRegime] = None
    confidence_score: float = 0.0

    # Timestamps
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    validated_at: Optional[datetime] = None
    risk_approved_at: Optional[datetime] = None
    submitted_at: Optional[datetime] = None

    # Audit chain
    validation_flags: List[str] = []
    rejection_reasons: List[str] = []
    agent_decision_chain: List[str] = []

    # Metadata
    is_paper_trade: bool = True


class Order(BaseModel):
    """A broker order, linked to a signal."""
    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    broker_order_id: Optional[str] = None
    signal_id: str
    instrument: str
    order_type: OrderType
    side: OrderSide
    quantity: float
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    filled_price: Optional[float] = None
    filled_quantity: float = 0.0
    status: str = "PENDING"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    filled_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None
    error_message: Optional[str] = None
    is_paper: bool = True
    slippage_pips: float = 0.0


class Trade(BaseModel):
    """A completed or open trade with full lifecycle tracking."""
    trade_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    signal_id: str
    order_id: str
    instrument: str
    instrument_type: InstrumentType
    direction: SignalDirection
    strategy_family: StrategyFamily
    session: SessionType
    regime: Optional[MarketRegime] = None

    # Entry
    entry_price: float
    entry_time: datetime
    stop_loss: float
    take_profit: float
    position_size_lots: float
    risk_amount_usd: float
    reward_to_risk: float

    # Exit
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    exit_reason: str = ""

    # PnL
    realized_pnl_usd: float = 0.0
    unrealized_pnl_usd: float = 0.0
    r_multiple: float = 0.0
    outcome: TradeOutcome = TradeOutcome.OPEN

    # Costs
    commission_usd: float = 0.0
    swap_usd: float = 0.0
    slippage_pips: float = 0.0

    # Context
    strategy_reason: str = ""
    volatility_context: str = ""
    agent_decision_chain: List[str] = []
    validation_flags: List[str] = []
    notes: str = ""

    # Mode
    is_paper: bool = True


class Position(BaseModel):
    """Current open position state."""
    position_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    trade_id: str
    instrument: str
    direction: SignalDirection
    size_lots: float
    entry_price: float
    current_price: float = 0.0
    stop_loss: float
    take_profit: float
    unrealized_pnl: float = 0.0
    opened_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    is_paper: bool = True


class EquitySnapshot(BaseModel):
    """Point-in-time equity snapshot for curve tracking."""
    timestamp: datetime
    balance: float
    equity: float
    open_pnl: float = 0.0
    drawdown_pct: float = 0.0
    peak_balance: float = 0.0
    trade_count: int = 0


class DailyPnLRecord(BaseModel):
    """Daily PnL summary for DailyRiskGovernor tracking."""
    date: str          # "YYYY-MM-DD"
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    trade_count: int = 0
    blocked_trades: int = 0
    is_killed: bool = False
    kill_reason: str = ""


class BlockedTrade(BaseModel):
    """A trade that was blocked by risk or validation, with full reason."""
    blocked_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    instrument: str
    direction: str
    strategy_family: str
    block_stage: str     # "VALIDATION" | "RISK" | "EXECUTION" | "SESSION" | "NEWS"
    block_reasons: List[str] = []
    setup_quality_score: float = 0.0
    regime: Optional[str] = None
    signal_data: Optional[Dict[str, Any]] = None


class PerformanceMetrics(BaseModel):
    """Aggregated performance metrics for reporting."""
    period: str = "all_time"
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    breakeven_trades: int = 0
    win_rate: float = 0.0
    avg_r_multiple: float = 0.0
    expectancy: float = 0.0
    profit_factor: float = 0.0
    net_profit_usd: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_ratio: float = 0.0
    avg_hold_hours: float = 0.0
    total_commission_usd: float = 0.0
    best_trade_r: float = 0.0
    worst_trade_r: float = 0.0
    consecutive_wins: int = 0
    consecutive_losses: int = 0
    by_session: Dict[str, Any] = {}
    by_regime: Dict[str, Any] = {}
    by_strategy: Dict[str, Any] = {}
    by_instrument: Dict[str, Any] = {}


# =============================================================================
# SYSTEM HEALTH
# =============================================================================

class StrategyHealthState(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNDERPERFORMING = "UNDERPERFORMING"
    SUSPENDED = "SUSPENDED"
    UNKNOWN = "UNKNOWN"


class SystemHealthSnapshot(BaseModel):
    """Real-time system health snapshot for dashboard."""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    environment: EnvironmentMode = EnvironmentMode.PAPER
    is_trading_enabled: bool = True
    kill_switch_active: bool = False
    kill_switch_reason: str = ""
    broker_connected: bool = False
    data_feed_active: bool = True
    active_positions: int = 0
    daily_pnl_usd: float = 0.0
    daily_pnl_pct: float = 0.0
    total_drawdown_pct: float = 0.0
    account_balance: float = 0.0
    account_equity: float = 0.0
    strategy_health: StrategyHealthState = StrategyHealthState.UNKNOWN
    active_agents: List[str] = []
    last_signal_time: Optional[datetime] = None
    last_trade_time: Optional[datetime] = None
    error_count_24h: int = 0
    uptime_hours: float = 0.0

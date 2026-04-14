"""
APEX MULTI-MARKET TJR ENGINE
Regime Detection Agent — Market state classification.

Classifies market into regime categories and routes
instruments to compatible strategy families only.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from domain.models import MarketRegime, InstrumentType, StrategyFamily
from core.market_context_filter import RegimeClassifier
from protocol.agent_protocol import (
    AgentMessage, AgentMessageType, MessagePriority, AgentName
)
from agents.base_agent import BaseAgent

logger = logging.getLogger(__name__)


# Regime → compatible strategy families
REGIME_STRATEGY_MAP: Dict[str, List[StrategyFamily]] = {
    MarketRegime.TRENDING_BULLISH.value: [
        StrategyFamily.TJR,
        StrategyFamily.TREND_FOLLOWING,
    ],
    MarketRegime.TRENDING_BEARISH.value: [
        StrategyFamily.TJR,
        StrategyFamily.TREND_FOLLOWING,
    ],
    MarketRegime.RANGING.value: [
        StrategyFamily.MEAN_REVERSION,
        StrategyFamily.MARKET_MAKING,
    ],
    MarketRegime.HIGH_VOLATILITY.value: [
        StrategyFamily.VOLATILITY_BREAKOUT,
    ],
    MarketRegime.UNSTABLE.value: [],            # No trading
    MarketRegime.NEWS_DRIVEN.value: [],         # No trading
    MarketRegime.ILLIQUID.value: [],            # No trading
    MarketRegime.BINARY_EVENT.value: [
        StrategyFamily.EVENT_DRIVEN,
    ],
    MarketRegime.UNKNOWN.value: [
        StrategyFamily.TJR,   # Default attempt, will fail quality filter
    ],
    MarketRegime.MEAN_REVERTING.value: [
        StrategyFamily.MEAN_REVERSION,
    ],
}


class RegimeDetectionAgent(BaseAgent):
    """
    Classifies market regime and supplies strategy routing.
    """

    def __init__(self, ledger, state_store, config: dict):
        super().__init__(AgentName.REGIME_DETECTION, ledger, state_store, config)
        self.classifier = RegimeClassifier(adx_period=14)

    def run(self, context: Dict[str, Any]) -> List[AgentMessage]:
        messages = []

        candle_feeds = context.get("candle_feeds", {})
        tradeable = context.get("active_instruments", [])
        regime_context = {}

        for instrument_info in tradeable:
            symbol = instrument_info.get("symbol", "")
            inst_type = instrument_info.get("type", "FOREX")
            candles = candle_feeds.get(symbol, [])

            if len(candles) < 30:
                regime = MarketRegime.UNKNOWN
            else:
                regime = self.classifier.classify(candles)

            # Determine eligible strategies
            eligible = REGIME_STRATEGY_MAP.get(regime.value, [StrategyFamily.TJR])
            eligible_names = [s.value for s in eligible]

            regime_context[symbol] = {
                "regime": regime.value,
                "eligible_strategies": eligible_names,
                "tradeable": len(eligible) > 0
            }

            msg = self.emit(
                message_type=AgentMessageType.REGIME_CLASSIFICATION,
                payload={
                    "instrument": symbol,
                    "regime": regime.value,
                    "eligible_strategies": eligible_names,
                    "tradeable": len(eligible) > 0
                },
                instrument=symbol,
                instrument_type=inst_type,
                regime_context=regime.value,
                confidence=0.75,
                final_status="CLASSIFIED"
            )
            messages.append(msg)

        self.state_store.set("regime_context", regime_context)
        logger.info(f"[RegimeDetection] Classified {len(regime_context)} instruments")
        return messages

"""
APEX MULTI-MARKET TJR ENGINE
Market Sentinel Agent — Continuous market environment monitoring.

Responsibilities:
- Scan all supported markets continuously
- Monitor trading sessions
- Detect volatility spikes
- Detect spread abnormalities
- Detect liquidity instability
- Identify tradeable vs non-tradeable environments
- Raise environment risk flags
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from domain.models import MarketRegime, Candle
from core.market_context_filter import MarketContextFilter, VolatilityAnalyzer, RegimeClassifier
from protocol.agent_protocol import (
    AgentMessage, AgentMessageType, MessagePriority, AgentName
)
from agents.base_agent import BaseAgent

logger = logging.getLogger(__name__)


class MarketSentinelAgent(BaseAgent):
    """
    Continuously monitors market environment health.
    Raises environment risk flags when conditions deteriorate.
    """

    def __init__(self, ledger, state_store, config: dict):
        super().__init__(AgentName.MARKET_SENTINEL, ledger, state_store, config)

        sentinel_cfg = config.get("agents", {}).get("market_sentinel", {})
        self.volatility_spike_multiplier = sentinel_cfg.get(
            "volatility_spike_atr_multiplier", 3.0
        )
        self.spread_spike_multiplier = sentinel_cfg.get("spread_spike_multiplier", 3.0)

        self.vol_analyzer = VolatilityAnalyzer(atr_period=14)
        self.regime_classifier = RegimeClassifier(adx_period=14)
        self.context_filter = MarketContextFilter(config)

    def run(self, context: Dict[str, Any]) -> List[AgentMessage]:
        """
        Evaluate market conditions for all active instruments.
        Emits MARKET_SCAN_RESULT for each instrument.
        """
        messages = []

        candle_feeds = context.get("candle_feeds", {})
        instruments = context.get("active_instruments", [])
        current_spreads = context.get("current_spreads", {})
        current_time = context.get("current_time", datetime.now(timezone.utc))

        scan_summary = {
            "tradeable": [],
            "blocked": [],
            "total_scanned": len(instruments),
            "timestamp": current_time.isoformat()
        }

        for instrument_info in instruments:
            symbol = instrument_info.get("symbol", "")
            inst_type = instrument_info.get("type", "FOREX")

            candles: List[Candle] = candle_feeds.get(symbol, [])
            if len(candles) < 30:
                scan_summary["blocked"].append({
                    "instrument": symbol,
                    "reason": "Insufficient candle data"
                })
                continue

            spread = current_spreads.get(symbol, 1.5)

            # Run context evaluation
            eval_result = self.context_filter.evaluate(
                candles=candles,
                instrument=symbol,
                current_spread_pips=spread,
                check_time=current_time
            )

            summary = eval_result.get("summary", {})
            regime = summary.get("regime", MarketRegime.UNKNOWN)
            vol_class = summary.get("volatility_class", "UNKNOWN")
            all_passed = summary.get("all_passed", True)
            blocked_reasons = summary.get("blocked_reasons", [])

            instrument_status = {
                "instrument": symbol,
                "regime": regime.value if hasattr(regime, "value") else str(regime),
                "volatility": vol_class,
                "spread": spread,
                "tradeable": all_passed,
                "blocked_reasons": blocked_reasons
            }

            if all_passed:
                scan_summary["tradeable"].append(instrument_status)
            else:
                scan_summary["blocked"].append(instrument_status)

            # Emit per-instrument result
            msg = self.emit(
                message_type=AgentMessageType.MARKET_SCAN_RESULT,
                payload=instrument_status,
                priority=(MessagePriority.HIGH if not all_passed else MessagePriority.NORMAL),
                instrument=symbol,
                instrument_type=inst_type,
                regime_context=regime.value if hasattr(regime, "value") else str(regime),
                final_status="BLOCKED" if not all_passed else "TRADEABLE",
                rejection_reasons=blocked_reasons
            )
            messages.append(msg)

        # Update state store with scan results
        self.state_store.set("market_scan_summary", scan_summary)
        self.state_store.set("tradeable_instruments",
                             [i["instrument"] for i in scan_summary["tradeable"]])

        logger.info(
            f"[MarketSentinel] Scan: {len(scan_summary['tradeable'])} tradeable, "
            f"{len(scan_summary['blocked'])} blocked"
        )

        return messages

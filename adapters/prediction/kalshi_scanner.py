"""
APEX MULTI-MARKET TJR ENGINE
Prediction Market Adapter — Kalshi Scanner

EXACT RULES (DO NOT MODIFY):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ASSET CLASS : PREDICTION
STRATEGY    : Prediction Market Arbitrage (Kalshi)

SETUP REQUIREMENTS:
  1. Connect to Kalshi REST API (demo or prod)
     Base URL demo: https://demo-api.kalshi.co/trade-api/v2
     Base URL prod: https://trading-api.kalshi.co/trade-api/v2

  2. Scan all active markets every 60 seconds
     Filter markets where:
       - status == "active"
       - close_time > now (not expired)
       - volume_24h >= 1000 (minimum liquidity)

  3. Arbitrage detection:
       For binary markets with YES/NO prices (0–1 range, in ¢):
         yes_price + no_price should = 100¢ (no-arb condition)
         If yes_price + no_price < 98¢ → BUY both sides (riskless arb ≥ 2¢)
         If yes_price + no_price > 102¢ → SELL both sides (implied liability)

  4. Pricing model:
       fair_value_yes = estimated_probability × 100  (in cents)
       edge_yes = fair_value_yes − yes_price  (positive = buy YES cheap)
       edge_no  = (100 − fair_value_yes) − no_price (positive = buy NO cheap)
       Minimum edge to signal: 5¢ (configurable)

  5. Kelly sizing:
       f* = (b×p − q) / b  where b = payout_ratio, p = win_prob, q = 1-p
       Capped at 2% of account per market (max_kelly_fraction)
       Minimum position: $10

  6. Signal output:
       Produces StrategySignal with:
         direction = "BUY_YES" | "BUY_NO" | "NONE"
         entry_price = current ask price
         stop_loss   = 0 (binary — can go to 0)
         take_profit = 1.00 (full payout)
         position_size = kelly-sized units

  7. Exit rules:
       Close when fair_value converges within 2¢ of current price
       Or at market close/expiration
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ─── EXACT CONSTANTS (DO NOT CHANGE) ─────────────────────────────────────────
KALSHI_DEMO_URL  = "https://demo-api.kalshi.co/trade-api/v2"
KALSHI_PROD_URL  = "https://trading-api.kalshi.co/trade-api/v2"

MIN_VOLUME_24H           = 1000     # minimum 24h volume for liquidity
ARB_OPPORTUNITY_THRESHOLD = 98      # YES + NO < 98¢ → riskless arb
ARB_LIABILITY_THRESHOLD  = 102      # YES + NO > 102¢ → short opportunity
MIN_EDGE_CENTS           = 5        # minimum fair-value edge in cents to signal
MAX_KELLY_FRACTION       = 0.02     # max 2% of account per market
MIN_POSITION_USD         = 10.0     # minimum position size in dollars
SCAN_INTERVAL_SECONDS    = 60       # scan interval
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class KalshiMarket:
    """Represents a single active Kalshi binary market."""
    ticker: str                          # e.g. "KXETH-25DEC31-T2500"
    title: str                           # human-readable title
    status: str                          # active | closed | settled
    yes_ask: float                       # best ask for YES (cents)
    yes_bid: float                       # best bid for YES (cents)
    no_ask: float                        # best ask for NO (cents)
    no_bid: float                        # best bid for NO (cents)
    volume_24h: int                      # 24h volume (contracts)
    open_interest: int                   # open interest
    close_time: datetime                 # market expiration
    category: str                        # finance | sports | politics | etc.
    result: Optional[str] = None        # YES | NO | None (pending)
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def yes_no_sum(self) -> float:
        """Sum of best ask prices. Should be ~100¢ in efficient markets."""
        return self.yes_ask + self.no_ask

    @property
    def has_arb(self) -> bool:
        """True if riskless arb opportunity exists (buy both sides)."""
        return self.yes_no_sum < ARB_OPPORTUNITY_THRESHOLD

    @property
    def is_liquid(self) -> bool:
        """True if market meets minimum liquidity requirements."""
        return self.volume_24h >= MIN_VOLUME_24H

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "title": self.title,
            "status": self.status,
            "yes_ask": self.yes_ask,
            "yes_bid": self.yes_bid,
            "no_ask": self.no_ask,
            "no_bid": self.no_bid,
            "yes_no_sum": self.yes_no_sum,
            "has_arb": self.has_arb,
            "volume_24h": self.volume_24h,
            "open_interest": self.open_interest,
            "close_time": self.close_time.isoformat(),
            "category": self.category,
        }


@dataclass
class KalshiOpportunity:
    """Represents a detected prediction market opportunity."""
    market: KalshiMarket
    direction: str                       # BUY_YES | BUY_NO | ARB_BOTH
    edge_cents: float                    # fair-value edge in cents
    fair_value_yes: float               # estimated YES probability × 100
    kelly_fraction: float               # Kelly criterion sizing
    position_size_usd: float           # dollar size
    confidence: float                   # 0.0–1.0
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        return {
            "ticker": self.market.ticker,
            "title": self.market.title,
            "direction": self.direction,
            "edge_cents": round(self.edge_cents, 2),
            "fair_value_yes": round(self.fair_value_yes, 2),
            "yes_ask": self.market.yes_ask,
            "no_ask": self.market.no_ask,
            "kelly_fraction": round(self.kelly_fraction, 4),
            "position_size_usd": round(self.position_size_usd, 2),
            "confidence": round(self.confidence, 3),
            "detected_at": self.detected_at.isoformat(),
        }


class KalshiScanner:
    """
    Kalshi prediction market scanner.

    Connects to Kalshi REST API, scans active markets,
    and identifies arbitrage/mispriced opportunities.

    Credentials: Set APEX_KALSHI_EMAIL and APEX_KALSHI_PASSWORD (or APEX_KALSHI_API_KEY)
                 in environment variables.
    """

    def __init__(self, config: dict = None, environment: str = "demo"):
        self._config = config or {}
        self._environment = environment
        self._base_url = KALSHI_DEMO_URL if environment == "demo" else KALSHI_PROD_URL
        self._session_token: Optional[str] = None
        self._authenticated = False

        # Load credentials
        self._email    = os.getenv("APEX_KALSHI_EMAIL", "")
        self._password = os.getenv("APEX_KALSHI_PASSWORD", "")
        self._api_key  = os.getenv("APEX_KALSHI_API_KEY", "")

        # Scan history
        self._last_scan: Optional[datetime] = None
        self._opportunities: List[KalshiOpportunity] = []
        self._scanned_markets: List[KalshiMarket] = []

    # ------------------------------------------------------------------
    # AUTHENTICATION
    # ------------------------------------------------------------------

    def authenticate(self) -> tuple[bool, str]:
        """
        Authenticate with Kalshi API.
        Returns (True, msg) on success, (False, reason) on failure.
        """
        if not (self._email and self._password) and not self._api_key:
            return (
                False,
                "Kalshi credentials not set. Set APEX_KALSHI_EMAIL + APEX_KALSHI_PASSWORD "
                "or APEX_KALSHI_API_KEY in environment."
            )

        try:
            import httpx
            if self._api_key:
                # API key auth (newer Kalshi auth method)
                self._session_token = self._api_key
                self._authenticated = True
                return True, f"Kalshi authenticated via API key ({self._environment})"

            # Username/password login
            resp = httpx.post(
                f"{self._base_url}/log_in",
                json={"email": self._email, "password": self._password},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                self._session_token = data.get("token", "")
                self._authenticated = True
                return True, f"Kalshi authenticated ({self._environment})"
            else:
                return False, f"Kalshi login failed: HTTP {resp.status_code} {resp.text[:200]}"

        except ImportError:
            return False, "httpx not installed. Run: pip install httpx"
        except Exception as exc:
            return False, f"Kalshi auth exception: {exc}"

    # ------------------------------------------------------------------
    # MARKET SCANNING
    # ------------------------------------------------------------------

    def scan(self, categories: List[str] = None) -> List[KalshiOpportunity]:
        """
        Scan active Kalshi markets and return opportunities.

        Args:
            categories: filter by category (finance, crypto, politics, etc.)
                        None = scan all categories

        Returns:
            List of KalshiOpportunity objects sorted by edge descending.
        """
        markets = self._fetch_active_markets(categories)
        self._scanned_markets = markets
        self._last_scan = datetime.now(timezone.utc)

        opportunities = []
        for market in markets:
            opp = self._analyse_market(market)
            if opp:
                opportunities.append(opp)

        # Sort by edge descending (best opportunities first)
        opportunities.sort(key=lambda o: o.edge_cents, reverse=True)
        self._opportunities = opportunities

        logger.info(
            f"[KalshiScanner] Scan complete: {len(markets)} markets scanned, "
            f"{len(opportunities)} opportunities found"
        )
        return opportunities

    def _fetch_active_markets(self, categories: List[str] = None) -> List[KalshiMarket]:
        """
        Fetch all active markets from Kalshi REST API.
        Filters by status=active, close_time>now, volume>=MIN_VOLUME_24H.
        """
        if not self._authenticated:
            logger.warning("[KalshiScanner] Not authenticated — returning empty market list")
            return []

        try:
            import httpx
            headers = {}
            if self._session_token:
                headers["Authorization"] = f"Bearer {self._session_token}"

            params = {"status": "active", "limit": 1000}
            if categories:
                params["category"] = categories[0]  # API takes one at a time

            resp = httpx.get(
                f"{self._base_url}/markets",
                headers=headers,
                params=params,
                timeout=15,
            )

            if resp.status_code != 200:
                logger.warning(
                    f"[KalshiScanner] Market fetch failed: HTTP {resp.status_code}"
                )
                return []

            data = resp.json()
            raw_markets = data.get("markets", [])
            now = datetime.now(timezone.utc)

            markets = []
            for m in raw_markets:
                try:
                    close_str = m.get("close_time", "")
                    if close_str:
                        close_time = datetime.fromisoformat(
                            close_str.replace("Z", "+00:00")
                        )
                    else:
                        continue

                    if close_time <= now:
                        continue  # expired

                    volume_24h = m.get("volume_24h", 0) or 0
                    if volume_24h < MIN_VOLUME_24H:
                        continue  # insufficient liquidity

                    market = KalshiMarket(
                        ticker=m.get("ticker", ""),
                        title=m.get("title", ""),
                        status=m.get("status", ""),
                        yes_ask=float(m.get("yes_ask", 50)),
                        yes_bid=float(m.get("yes_bid", 50)),
                        no_ask=float(m.get("no_ask", 50)),
                        no_bid=float(m.get("no_bid", 50)),
                        volume_24h=int(volume_24h),
                        open_interest=int(m.get("open_interest", 0) or 0),
                        close_time=close_time,
                        category=m.get("category", ""),
                        raw=m,
                    )
                    markets.append(market)
                except Exception as exc:
                    logger.debug(f"[KalshiScanner] Failed to parse market: {exc}")

            return markets

        except Exception as exc:
            logger.error(f"[KalshiScanner] _fetch_active_markets failed: {exc}")
            return []

    def _analyse_market(self, market: KalshiMarket) -> Optional[KalshiOpportunity]:
        """
        Analyse a single market for opportunities.

        Rules (exact):
          1. Riskless arb: YES ask + NO ask < 98¢ → BUY_BOTH
          2. Fair-value edge ≥ MIN_EDGE_CENTS → BUY_YES or BUY_NO
          3. Kelly sizing capped at MAX_KELLY_FRACTION
        """
        # Rule 1: Riskless arbitrage
        if market.has_arb:
            arb_profit = 100 - market.yes_no_sum
            edge = arb_profit
            kelly = min(MAX_KELLY_FRACTION, edge / 100)
            return KalshiOpportunity(
                market=market,
                direction="ARB_BOTH",
                edge_cents=edge,
                fair_value_yes=50.0,  # irrelevant for pure arb
                kelly_fraction=kelly,
                position_size_usd=0.0,  # sized by caller using account_balance
                confidence=min(1.0, edge / 5),
            )

        # Rule 2: Fair-value edge
        fair_value_yes = self._estimate_fair_value(market)

        edge_yes = fair_value_yes - market.yes_ask
        edge_no  = (100 - fair_value_yes) - market.no_ask

        best_edge = max(edge_yes, edge_no)
        if best_edge < MIN_EDGE_CENTS:
            return None  # No significant edge

        direction = "BUY_YES" if edge_yes >= edge_no else "BUY_NO"
        p = fair_value_yes / 100.0  # win probability as decimal
        if direction == "BUY_NO":
            p = 1 - p
        q = 1 - p

        # Kelly formula: f* = (b×p − q) / b where b = payout ratio
        # Kalshi binary: payout = 100¢, cost = ask price
        if direction == "BUY_YES":
            cost = market.yes_ask
        else:
            cost = market.no_ask

        if cost <= 0 or cost >= 100:
            return None

        b = (100 - cost) / cost  # odds ratio
        kelly = (b * p - q) / b
        kelly = max(0, min(kelly, MAX_KELLY_FRACTION))  # clamp to [0, MAX_KELLY]

        if kelly <= 0:
            return None  # negative Kelly = no edge

        return KalshiOpportunity(
            market=market,
            direction=direction,
            edge_cents=best_edge,
            fair_value_yes=fair_value_yes,
            kelly_fraction=kelly,
            position_size_usd=0.0,  # sized by caller
            confidence=min(1.0, best_edge / 10),
        )

    def _estimate_fair_value(self, market: KalshiMarket) -> float:
        """
        Estimate fair-value YES probability for a market.

        Simple mid-price model (can be replaced with ML model):
          fair_value_yes = (yes_bid + yes_ask) / 2

        In production, this should use:
          - Event probability models
          - Historical base rates
          - Correlated asset prices (e.g. BTC price for crypto event markets)
        """
        mid_yes = (market.yes_bid + market.yes_ask) / 2.0
        return mid_yes

    # ------------------------------------------------------------------
    # POSITION SIZING
    # ------------------------------------------------------------------

    def size_opportunity(
        self, opp: KalshiOpportunity, account_balance: float
    ) -> float:
        """
        Apply Kelly sizing to an opportunity.

        Returns position size in USD.
        Minimum: MIN_POSITION_USD
        Maximum: account_balance × MAX_KELLY_FRACTION
        """
        max_usd = account_balance * MAX_KELLY_FRACTION
        sized = account_balance * opp.kelly_fraction
        return max(MIN_POSITION_USD, min(sized, max_usd))

    # ------------------------------------------------------------------
    # STATUS / DIAGNOSTICS
    # ------------------------------------------------------------------

    def status(self) -> dict:
        return {
            "authenticated": self._authenticated,
            "environment": self._environment,
            "base_url": self._base_url,
            "last_scan": self._last_scan.isoformat() if self._last_scan else None,
            "markets_scanned": len(self._scanned_markets),
            "opportunities_found": len(self._opportunities),
            "top_opportunities": [
                o.to_dict() for o in self._opportunities[:5]
            ],
        }

    def get_opportunities(self) -> List[dict]:
        return [o.to_dict() for o in self._opportunities]

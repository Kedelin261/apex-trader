"""
APEX MULTI-MARKET TJR ENGINE
IBKR EClient/EWrapper App Wrapper — ibapi integration layer.

This module wraps ibapi's EClient and EWrapper to provide a synchronous-friendly
interface for the IBKRConnector. It handles all async callbacks from TWS/Gateway
and exposes them as thread-safe state.

Only imported when ibapi is installed. IBKRConnector falls back gracefully
if this import fails (socket probe mode for CI environments).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.contract import Contract
    from ibapi.order import Order
    from ibapi.account_summary_tags import AccountSummaryTags
    IBAPI_AVAILABLE = True
except ImportError:
    IBAPI_AVAILABLE = False
    # Create stub classes so module loads without ibapi
    class EClient:
        def __init__(self, wrapper): pass
    class EWrapper:
        pass


class IBKRAppWrapper(EWrapper, EClient):
    """
    Combined EClient + EWrapper for synchronous-friendly IBKR API access.

    Key design patterns:
      - All async callbacks store results in thread-safe dicts/lists
      - Callers wait on threading.Event objects for specific data
      - All state is held in memory; no file I/O in this layer
    """

    ACCOUNT_SUMMARY_TIMEOUT = 10  # seconds
    CONTRACT_DETAILS_TIMEOUT = 10

    def __init__(self, account_id: str, client_id: int = 1):
        if IBAPI_AVAILABLE:
            EWrapper.__init__(self)
            EClient.__init__(self, wrapper=self)

        self.account_id = account_id
        self.client_id = client_id

        # nextValidId — set when connection is established
        self.next_order_id: Optional[int] = None
        self._next_id_event = threading.Event()

        # Account summary data
        self._account_summary: Dict[str, Any] = {}
        self._account_summary_event = threading.Event()
        self._account_summary_req_id = 9001

        # Positions
        self._positions: List[dict] = []
        self._positions_event = threading.Event()

        # Open orders
        self._open_orders: List[dict] = []
        self._open_orders_event = threading.Event()

        # Contract details
        self._contract_details: Dict[int, dict] = {}
        self._contract_details_events: Dict[int, threading.Event] = {}
        self._contract_details_req_id = 9100

        # Order status (shared with connector)
        self._order_status: Dict[int, dict] = {}
        self._order_events: Dict[int, threading.Event] = {}

        logger.info(
            f"[IBKRAppWrapper] Initialised: account={account_id} client_id={client_id}"
        )

    # ------------------------------------------------------------------
    # EWrapper CALLBACKS — Connection
    # ------------------------------------------------------------------

    def nextValidId(self, orderId: int):
        """Called by TWS when connection is established."""
        logger.info(f"[IBKRAppWrapper] nextValidId received: {orderId}")
        self.next_order_id = orderId
        self._next_id_event.set()

    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderReject=""):
        """Called for all errors and informational messages."""
        # Error codes 2000-2999 are warnings/informational
        if 2000 <= errorCode < 3000:
            logger.debug(f"[IBKRAppWrapper] INFO {errorCode}: {errorString} (reqId={reqId})")
        else:
            logger.error(
                f"[IBKRAppWrapper] ERROR reqId={reqId} code={errorCode}: {errorString}"
            )
            # Signal any waiting order event
            if reqId in self._order_events:
                self._order_status[reqId] = {
                    "status": "ERROR",
                    "error_code": errorCode,
                    "error_msg": errorString,
                }
                self._order_events[reqId].set()
            # Signal any waiting contract details event
            if reqId in self._contract_details_events:
                self._contract_details_events[reqId].set()

    def connectionClosed(self):
        logger.warning("[IBKRAppWrapper] Connection closed by TWS/Gateway")

    # ------------------------------------------------------------------
    # EWrapper CALLBACKS — Account Summary
    # ------------------------------------------------------------------

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        if account == self.account_id or not self.account_id:
            self._account_summary[tag] = value
            if tag == "currency":
                self._account_summary["currency"] = currency

    def accountSummaryEnd(self, reqId: int):
        self._account_summary_event.set()

    # ------------------------------------------------------------------
    # EWrapper CALLBACKS — Positions
    # ------------------------------------------------------------------

    def position(self, account: str, contract, pos: float, avgCost: float):
        self._positions.append({
            "account": account,
            "symbol": contract.symbol,
            "sec_type": contract.secType,
            "currency": contract.currency,
            "position": pos,
            "avgCost": avgCost,
            "unrealizedPNL": 0.0,  # Filled by pnl callback
        })

    def positionEnd(self):
        self._positions_event.set()

    # ------------------------------------------------------------------
    # EWrapper CALLBACKS — Open Orders
    # ------------------------------------------------------------------

    def openOrder(self, orderId: int, contract, order, orderState):
        self._open_orders.append({
            "orderId": orderId,
            "symbol": contract.symbol,
            "secType": contract.secType,
            "action": order.action,
            "totalQuantity": order.totalQuantity,
            "orderType": order.orderType,
            "lmtPrice": order.lmtPrice,
            "status": orderState.status,
        })

    def openOrderEnd(self):
        self._open_orders_event.set()

    # ------------------------------------------------------------------
    # EWrapper CALLBACKS — Order Status
    # ------------------------------------------------------------------

    def orderStatus(
        self, orderId: int, status: str, filled: float,
        remaining: float, avgFillPrice: float, permId: int,
        parentId: int, lastFillPrice: float, clientId: int,
        whyHeld: str, mktCapPrice: float
    ):
        self._order_status[orderId] = {
            "status": status,
            "filled": filled,
            "remaining": remaining,
            "avg_fill_price": avgFillPrice,
            "last_fill_price": lastFillPrice,
        }
        if orderId in self._order_events:
            if status in ("Filled", "Cancelled", "Inactive"):
                self._order_events[orderId].set()

    # ------------------------------------------------------------------
    # EWrapper CALLBACKS — Contract Details
    # ------------------------------------------------------------------

    def contractDetails(self, reqId: int, contractDetails):
        self._contract_details[reqId] = {
            "tradeable": True,  # if we get details, it's tradeable
            "min_tick": contractDetails.minTick,
            "long_name": contractDetails.longName,
            "category": contractDetails.category,
            "order_types": contractDetails.orderTypes,
        }

    def contractDetailsEnd(self, reqId: int):
        if reqId in self._contract_details_events:
            self._contract_details_events[reqId].set()

    # ------------------------------------------------------------------
    # PUBLIC API — Synchronous helpers
    # ------------------------------------------------------------------

    def get_account_summary(self, timeout: float = None) -> dict:
        """
        Request and return account summary (synchronous with timeout).
        Returns dict with TotalCashValue, NetLiquidation, etc.
        """
        if not IBAPI_AVAILABLE:
            return {}
        self._account_summary.clear()
        self._account_summary_event.clear()
        tags = "TotalCashValue,NetLiquidation,UnrealizedPnL,RealizedPnL,MaintMarginReq,AvailableFunds"
        self.reqAccountSummary(self._account_summary_req_id, "All", tags)
        self._account_summary_event.wait(timeout or self.ACCOUNT_SUMMARY_TIMEOUT)
        self.cancelAccountSummary(self._account_summary_req_id)
        return dict(self._account_summary)

    def get_positions(self, timeout: float = 10) -> List[dict]:
        """Request and return all positions (synchronous)."""
        if not IBAPI_AVAILABLE:
            return []
        self._positions.clear()
        self._positions_event.clear()
        self.reqPositions()
        self._positions_event.wait(timeout)
        self.cancelPositions()
        return list(self._positions)

    def get_open_orders(self, timeout: float = 10) -> List[dict]:
        """Request and return all open orders (synchronous)."""
        if not IBAPI_AVAILABLE:
            return []
        self._open_orders.clear()
        self._open_orders_event.clear()
        self.reqAllOpenOrders()
        self._open_orders_event.wait(timeout)
        return list(self._open_orders)

    def validate_contract(self, contract, timeout: float = None) -> dict:
        """
        Request contract details to validate it is tradeable.
        Returns dict with 'tradeable' bool and details.
        """
        if not IBAPI_AVAILABLE:
            return {"tradeable": True}

        from ibapi.contract import Contract as IBContract
        ib_c = IBContract()
        ib_c.symbol   = contract.ibkr_symbol
        ib_c.secType  = contract.sec_type
        ib_c.exchange = contract.exchange
        ib_c.currency = contract.currency
        if contract.expiry:
            ib_c.lastTradeDateOrContractMonth = contract.expiry
        if contract.multiplier:
            ib_c.multiplier = contract.multiplier

        req_id = self._contract_details_req_id
        self._contract_details_req_id += 1
        self._contract_details_events[req_id] = threading.Event()

        self.reqContractDetails(req_id, ib_c)
        self._contract_details_events[req_id].wait(
            timeout or self.CONTRACT_DETAILS_TIMEOUT
        )

        result = self._contract_details.get(req_id, {"tradeable": False, "error": "No details received"})
        self._contract_details_events.pop(req_id, None)
        return result

    def place_order_and_wait(
        self,
        order_id: int,
        ib_contract,
        ib_order,
        timeout: float = 30,
    ) -> dict:
        """
        Place an order and wait for a status update.
        Returns status dict with 'status', 'filled', 'avg_fill_price'.
        """
        if not IBAPI_AVAILABLE:
            return {"status": "PAPER", "filled": ib_order.totalQuantity, "avg_fill_price": 0.0}

        event = threading.Event()
        self._order_events[order_id] = event
        self.placeOrder(order_id, ib_contract, ib_order)
        event.wait(timeout)
        result = self._order_status.get(order_id, {"status": "TIMEOUT", "filled": 0, "avg_fill_price": 0.0})
        self._order_events.pop(order_id, None)
        return result

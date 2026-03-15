from __future__ import annotations

import copy
import threading
import time
from typing import Any, Dict, List, Optional, Set


class SupportState:
    """
    Thread-safe in-memory state for SupportLayerV1.

    Domains:
        - account_state
        - positions
        - orders
        - executions
        - market_prices
        - subscribed_symbols
        - timestamps / health
    """

    def __init__(self, environment: str) -> None:
        self._lock = threading.RLock()
        self.environment = environment

        self.account_state: Dict[str, Any] = {
            "environment": environment,
            "available_balance": None,
            "wallet_balance": None,
            "equity": None,
            "total_perp_upl_private": None,
            "coins": {},
            "updated_time_local": None,
            "updated_time_exchange": None,
        }

        self.positions: Dict[str, Dict[str, Any]] = {}
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.executions: List[Dict[str, Any]] = []
        self.execution_ids: Set[str] = set()

        self.market_prices: Dict[str, Dict[str, Any]] = {}
        self.subscribed_symbols: Set[str] = set()

        self.timestamps: Dict[str, Any] = {
            "wallet": None,
            "position": None,
            "order": None,
            "execution": None,
            "public_price_by_symbol": {},
            "private_ws_started": None,
            "public_ws_started": None,
            "rest_bootstrap": None,
            "last_private_event": None,
            "last_public_event": None,
        }

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        try:
            if value in ("", None):
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_side(side: Any) -> Optional[str]:
        if side is None:
            return None
        side_str = str(side).strip().lower()
        if side_str in {"buy", "long"}:
            return "Buy"
        if side_str in {"sell", "short"}:
            return "Sell"
        return None

    @staticmethod
    def _calc_live_pnl(
        side: Optional[str],
        size: Any,
        entry_price: Any,
        live_price: Any,
    ) -> Optional[float]:
        side_norm = SupportState._normalize_side(side)
        qty = SupportState._to_float(size)
        entry = SupportState._to_float(entry_price)
        live = SupportState._to_float(live_price)

        if side_norm is None or qty is None or entry is None or live is None:
            return None

        if side_norm == "Buy":
            return (live - entry) * qty
        if side_norm == "Sell":
            return (entry - live) * qty
        return None

    def mark_private_ws_started(self) -> None:
        with self._lock:
            self.timestamps["private_ws_started"] = self._now_ms()

    def mark_public_ws_started(self) -> None:
        with self._lock:
            self.timestamps["public_ws_started"] = self._now_ms()

    def mark_rest_bootstrap(self) -> None:
        with self._lock:
            self.timestamps["rest_bootstrap"] = self._now_ms()

    def update_wallet(self, wallet_update: Dict[str, Any]) -> None:
        now_ms = self._now_ms()
        with self._lock:
            self.account_state.update(
                {
                    "environment": self.environment,
                    "available_balance": wallet_update.get("available_balance"),
                    "wallet_balance": wallet_update.get("wallet_balance"),
                    "equity": wallet_update.get("equity"),
                    "total_perp_upl_private": wallet_update.get("total_perp_upl_private"),
                    "coins": copy.deepcopy(wallet_update.get("coins", {})),
                    "updated_time_local": now_ms,
                    "updated_time_exchange": wallet_update.get("updated_time_exchange"),
                }
            )
            self.timestamps["wallet"] = now_ms
            self.timestamps["last_private_event"] = now_ms

    def update_position(self, position_update: Dict[str, Any]) -> None:
        symbol = position_update["symbol"]
        now_ms = self._now_ms()

        with self._lock:
            live_price = None
            if symbol in self.market_prices:
                live_price = self.market_prices[symbol].get("price")

            self.positions[symbol] = {
                "symbol": symbol,
                "side": self._normalize_side(position_update.get("side")),
                "size": position_update.get("size"),
                "entry_price": position_update.get("entry_price"),
                "private_mark_price": position_update.get("private_mark_price"),
                "private_unrealized_pnl": position_update.get("private_unrealized_pnl"),
                "position_value": position_update.get("position_value"),
                "liq_price": position_update.get("liq_price"),
                "updated_time_exchange": position_update.get("updated_time_exchange"),
                "position_updated_time_local": now_ms,
                "live_market_price": live_price,
                "live_unrealized_pnl": self._calc_live_pnl(
                    side=position_update.get("side"),
                    size=position_update.get("size"),
                    entry_price=position_update.get("entry_price"),
                    live_price=live_price,
                ),
                "live_recalc_time_local": now_ms if live_price is not None else None,
            }

            self.timestamps["position"] = now_ms
            self.timestamps["last_private_event"] = now_ms

    def remove_position(self, symbol: str) -> None:
        now_ms = self._now_ms()
        with self._lock:
            self.positions.pop(symbol, None)
            self.timestamps["position"] = now_ms
            self.timestamps["last_private_event"] = now_ms

    def update_order(self, order_update: Dict[str, Any]) -> None:
        order_id = order_update["order_id"]
        now_ms = self._now_ms()

        with self._lock:
            self.orders[order_id] = {
                "order_id": order_id,
                "symbol": order_update.get("symbol"),
                "side": self._normalize_side(order_update.get("side")),
                "order_type": order_update.get("order_type"),
                "price": order_update.get("price"),
                "qty": order_update.get("qty"),
                "order_status": order_update.get("order_status"),
                "cum_exec_qty": order_update.get("cum_exec_qty"),
                "avg_price": order_update.get("avg_price"),
                "updated_time_exchange": order_update.get("updated_time_exchange"),
                "updated_time_local": now_ms,
            }
            self.timestamps["order"] = now_ms
            self.timestamps["last_private_event"] = now_ms

    def remove_closed_orders(self) -> None:
        closed_statuses = {"Filled", "Cancelled", "Rejected", "Deactivated"}
        with self._lock:
            self.orders = {
                oid: order
                for oid, order in self.orders.items()
                if order.get("order_status") not in closed_statuses
            }

    def add_execution(self, execution_update: Dict[str, Any], max_items: int = 500) -> None:
        exec_id = execution_update.get("exec_id")
        now_ms = self._now_ms()

        with self._lock:
            if exec_id and exec_id in self.execution_ids:
                return

            item = {
                "symbol": execution_update.get("symbol"),
                "side": self._normalize_side(execution_update.get("side")),
                "exec_price": execution_update.get("exec_price"),
                "exec_qty": execution_update.get("exec_qty"),
                "exec_fee": execution_update.get("exec_fee"),
                "exec_pnl": execution_update.get("exec_pnl"),
                "exec_time": execution_update.get("exec_time"),
                "order_id": execution_update.get("order_id"),
                "exec_id": exec_id,
                "updated_time_local": now_ms,
            }

            self.executions.append(item)
            if exec_id:
                self.execution_ids.add(exec_id)

            if len(self.executions) > max_items:
                removed = self.executions[:-max_items]
                self.executions = self.executions[-max_items:]
                for item_removed in removed:
                    rid = item_removed.get("exec_id")
                    if rid and rid not in {x.get("exec_id") for x in self.executions}:
                        self.execution_ids.discard(rid)

            self.timestamps["execution"] = now_ms
            self.timestamps["last_private_event"] = now_ms

    def update_market_price(self, symbol: str, price: float) -> None:
        now_ms = self._now_ms()

        with self._lock:
            self.market_prices[symbol] = {
                "symbol": symbol,
                "price": price,
                "updated_time_local": now_ms,
            }
            self.timestamps["public_price_by_symbol"][symbol] = now_ms
            self.timestamps["last_public_event"] = now_ms

            if symbol in self.positions:
                pos = self.positions[symbol]
                pos["live_market_price"] = price
                pos["live_unrealized_pnl"] = self._calc_live_pnl(
                    side=pos.get("side"),
                    size=pos.get("size"),
                    entry_price=pos.get("entry_price"),
                    live_price=price,
                )
                pos["live_recalc_time_local"] = now_ms

    def add_subscribed_symbol(self, symbol: str) -> None:
        with self._lock:
            self.subscribed_symbols.add(symbol)

    def remove_subscribed_symbol(self, symbol: str) -> None:
        with self._lock:
            self.subscribed_symbols.discard(symbol)
            self.market_prices.pop(symbol, None)
            self.timestamps["public_price_by_symbol"].pop(symbol, None)

            if symbol in self.positions:
                self.positions[symbol]["live_market_price"] = None
                self.positions[symbol]["live_unrealized_pnl"] = None
                self.positions[symbol]["live_recalc_time_local"] = None

    def cleanup_orphan_price_state(self) -> None:
        """
        Remove public price cache/timestamps for symbols that are no longer subscribed
        and no longer open.
        """
        with self._lock:
            valid_symbols = self.subscribed_symbols | set(self.positions.keys())

            for symbol in list(self.market_prices.keys()):
                if symbol not in valid_symbols:
                    self.market_prices.pop(symbol, None)

            for symbol in list(self.timestamps["public_price_by_symbol"].keys()):
                if symbol not in valid_symbols:
                    self.timestamps["public_price_by_symbol"].pop(symbol, None)


    def get_account_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self.account_state)

    def get_positions_snapshot(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self.positions)

    def get_orders_snapshot(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self.orders)

    def get_recent_executions(self, limit: int = 10) -> List[Dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self.executions[-limit:])

    def get_subscribed_symbols(self) -> Set[str]:
        with self._lock:
            return set(self.subscribed_symbols)

    def get_open_symbols(self) -> Set[str]:
        with self._lock:
            return set(self.positions.keys())

    def get_live_price(self, symbol: str) -> Optional[float]:
        with self._lock:
            item = self.market_prices.get(symbol)
            return None if item is None else item.get("price")

    def get_live_pnl(self, symbol: str) -> Optional[float]:
        with self._lock:
            pos = self.positions.get(symbol)
            return None if pos is None else pos.get("live_unrealized_pnl")

    def get_total_live_pnl(self) -> float:
        with self._lock:
            total = 0.0
            for pos in self.positions.values():
                pnl = pos.get("live_unrealized_pnl")
                if pnl is not None:
                    total += pnl
            return total

    def get_timestamps_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self.timestamps)

    def get_health_snapshot(
        self,
        private_stale_sec: int = 30,
        public_stale_sec: int = 15,
    ) -> Dict[str, Any]:
        now_ms = self._now_ms()
        with self._lock:
            last_private = self.timestamps.get("last_private_event")
            last_public = self.timestamps.get("last_public_event")

            private_stale = (
                last_private is None or (now_ms - last_private) > private_stale_sec * 1000
            )
            public_stale = (
                last_public is None or (now_ms - last_public) > public_stale_sec * 1000
            )

            stale_symbols: List[str] = []
            for symbol in self.subscribed_symbols:
                ts = self.timestamps["public_price_by_symbol"].get(symbol)
                if ts is None or (now_ms - ts) > public_stale_sec * 1000:
                    stale_symbols.append(symbol)

            return {
                "environment": self.environment,
                "private_ws_started": self.timestamps.get("private_ws_started"),
                "public_ws_started": self.timestamps.get("public_ws_started"),
                "rest_bootstrap": self.timestamps.get("rest_bootstrap"),
                "last_private_event": last_private,
                "last_public_event": last_public,
                "private_stream_stale": private_stale,
                "public_stream_stale": public_stale,
                "stale_symbols": sorted(stale_symbols),
                "tracked_public_symbols": sorted(self.subscribed_symbols),
            }
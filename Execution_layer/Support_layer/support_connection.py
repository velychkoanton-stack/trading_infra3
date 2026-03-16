from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

from pybit.unified_trading import WebSocket

from Common.exchange.bybit_client import create_bybit_client
from Common.config.api_loader import load_api_file
from Common.config.path_config import get_api_file_path

try:
    from .support_state import SupportState
except ImportError:
    from support_state import SupportState


logger = logging.getLogger(__name__)


class SupportConnection:
    """
    Exchange connectivity and state synchronization layer.

    Responsibilities:
        - load credentials
        - build CCXT client
        - bootstrap open positions / wallet via REST
        - build private and public WS clients
        - normalize incoming events into SupportState
        - manage dynamic ticker subscriptions
    """

    def __init__(
        self,
        state: SupportState,
        environment: str = "demo",
        api_file_name: str = "api_credentials.txt",
        summary_log_prefix: str = "SupportLayerV1",
    ) -> None:
        self.state = state
        self.environment = environment.lower().strip()
        self.api_file_name = api_file_name
        self.summary_log_prefix = summary_log_prefix

        if self.environment not in {"demo", "real"}:
            raise ValueError("environment must be either 'demo' or 'real'")

        self.credentials_file = Path(api_file_name)
        self.api_key: Optional[str] = None
        self.api_secret: Optional[str] = None

        self.exchange = None
        self.private_ws: Optional[WebSocket] = None
        self.public_ws: Optional[WebSocket] = None

        self._stop_event = threading.Event()
        self._subscription_manager_thread: Optional[threading.Thread] = None

        self._watchdog_thread: Optional[threading.Thread] = None
        self._reconnect_lock = threading.Lock()
        self._reconnect_in_progress = False
        self._last_restart_ts = 0.0



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

    def load_credentials(self) -> None:
        api_path = get_api_file_path(self.api_file_name)
        api_config = load_api_file(api_path)

        self.api_key = api_config.get("API_KEY", "")
        self.api_secret = api_config.get("API_SECRET", "")

        if not self.api_key or not self.api_secret:
            raise ValueError(
                f"API_KEY / API_SECRET missing in api file: {api_path}"
            )

        raw: Dict[str, str] = {}
        if self.credentials_file.exists():
            with open(self.credentials_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    raw[key.strip()] = value.strip()

        self.api_key = raw.get("api_key") or raw.get("API_KEY")
        self.api_secret = raw.get("api_secret") or raw.get("API_SECRET")

    def build_ccxt(self):
        self.exchange = create_bybit_client(
            api_file_name=self.api_file_name,
            demo=(self.environment == "demo"),
        )
        self.api_key = self.exchange.apiKey
        self.api_secret = self.exchange.secret
        return self.exchange    

    def bootstrap_rest(self) -> None:
        if self.exchange is None:
            raise RuntimeError("CCXT exchange must be built before REST bootstrap")

        logger.info("%s | REST bootstrap started", self.summary_log_prefix)

        self._bootstrap_wallet()
        self._bootstrap_positions()
        self.state.mark_rest_bootstrap()

        logger.info("%s | REST bootstrap finished", self.summary_log_prefix)

    def _bootstrap_wallet(self) -> None:
        try:
            balance = self.exchange.fetch_balance()

            total_usdt = None
            free_usdt = None

            if isinstance(balance, dict):
                total_usdt = (balance.get("total") or {}).get("USDT")
                free_usdt = (balance.get("free") or {}).get("USDT")

            coins = {}
            if total_usdt is not None:
                coins["USDT"] = {
                    "walletBalance": total_usdt,
                    "equity": total_usdt,
                    "usdValue": total_usdt,
                    "unrealisedPnl": None,
                }

            self.state.update_wallet(
                {
                    "available_balance": free_usdt,
                    "wallet_balance": total_usdt,
                    "equity": total_usdt,
                    "total_perp_upl_private": None,
                    "coins": coins,
                    "updated_time_exchange": None,
                }
            )

        except Exception as exc:
            logger.warning("%s | wallet bootstrap failed: %s", self.summary_log_prefix, exc)

    def _bootstrap_positions(self) -> None:
        try:
            positions = self.exchange.fetch_positions()

            for pos in positions:
                info = pos.get("info", {}) or {}
                symbol = info.get("symbol")
                if not symbol:
                    continue

                contracts = pos.get("contracts")
                contracts_float = self._to_float(contracts)
                if contracts_float is None or contracts_float == 0:
                    continue

                self.state.update_position(
                    {
                        "symbol": symbol,
                        "side": self._normalize_side(pos.get("side") or info.get("side")),
                        "size": contracts_float,
                        "entry_price": self._to_float(
                            pos.get("entryPrice") or info.get("avgPrice") or info.get("entryPrice")
                        ),
                        "private_mark_price": self._to_float(pos.get("markPrice") or info.get("markPrice")),
                        "private_unrealized_pnl": self._to_float(
                            pos.get("unrealizedPnl") or info.get("unrealisedPnl")
                        ),
                        "position_value": self._to_float(pos.get("notional") or info.get("positionValue")),
                        "liq_price": self._to_float(info.get("liqPrice")),
                        "updated_time_exchange": info.get("updatedTime"),
                    }
                )

        except Exception as exc:
            logger.warning("%s | positions bootstrap failed: %s", self.summary_log_prefix, exc)

    def start(self) -> None:
        self.load_credentials()
        self.build_ccxt()
        self.bootstrap_rest()
        self._start_private_ws()
        self._start_public_ws()
        self._start_subscription_manager()
        self._start_watchdog()

    def stop(self) -> None:
        self._stop_event.set()

        try:
            if self.private_ws is not None and hasattr(self.private_ws, "exit"):
                self.private_ws.exit()
        except Exception as exc:
            logger.warning("%s | private ws exit failed: %s", self.summary_log_prefix, exc)

        try:
            if self.public_ws is not None and hasattr(self.public_ws, "exit"):
                self.public_ws.exit()
        except Exception as exc:
            logger.warning("%s | public ws exit failed: %s", self.summary_log_prefix, exc)

    def _start_private_ws(self) -> None:
        if not self.api_key or not self.api_secret:
            raise RuntimeError("Credentials must be loaded before starting private WS")

        self.private_ws = WebSocket(
            testnet=False,
            demo=(self.environment == "demo"),
            channel_type="private",
            api_key=self.api_key,
            api_secret=self.api_secret,
        )
            

        self.private_ws.wallet_stream(callback=self._on_wallet)
        self.private_ws.position_stream(callback=self._on_position)
        self.private_ws.order_stream(callback=self._on_order)
        self.private_ws.execution_stream(callback=self._on_execution)

        self.state.mark_private_ws_started()
        logger.info("%s | private ws started | env=%s", self.summary_log_prefix, self.environment)

    def _start_public_ws(self) -> None:
        self.public_ws = WebSocket(
            testnet=False,
            channel_type="linear",
        )

        self.state.mark_public_ws_started()
        logger.info("%s | public ws started", self.summary_log_prefix)

        self._sync_public_subscriptions()

    def _start_subscription_manager(self) -> None:
        self._subscription_manager_thread = threading.Thread(
            target=self._subscription_manager_loop,
            name="support-subscription-manager",
            daemon=True,
        )
        self._subscription_manager_thread.start()

    def _subscription_manager_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._sync_public_subscriptions()
                self.state.remove_closed_orders()
                self.state.cleanup_orphan_price_state()
            except Exception as exc:
                logger.warning("%s | subscription sync failed: %s", self.summary_log_prefix, exc)
            time.sleep(1.0)

    def _sync_public_subscriptions(self) -> None:
        if self.public_ws is None:
            return

        open_symbols = self.state.get_open_symbols()
        subscribed_symbols = self.state.get_subscribed_symbols()

        to_subscribe = sorted(open_symbols - subscribed_symbols)
        to_unsubscribe = sorted(subscribed_symbols - open_symbols)

        for symbol in to_subscribe:
            self._subscribe_symbol(symbol)

        for symbol in to_unsubscribe:
            self._unsubscribe_symbol(symbol)

    def _subscribe_symbol(self, symbol: str) -> None:
        if self.public_ws is None:
            return

        logger.info("%s | subscribe public ticker %s", self.summary_log_prefix, symbol)
        self.public_ws.ticker_stream(symbol=symbol, callback=self._make_ticker_handler(symbol))
        self.state.add_subscribed_symbol(symbol)

    def _unsubscribe_symbol(self, symbol: str) -> None:
        if self.public_ws is None:
            return

        logger.info("%s | unsubscribe public ticker %s", self.summary_log_prefix, symbol)

        try:
            topics = self.public_ws.get_subscription_topics()
            topic = next(
                (
                    topic_name
                    for topic_name in topics
                    if topic_name.endswith(symbol) and "tickers." in topic_name
                ),
                None,
            )
            if topic is not None:
                self.public_ws.unsubscribe(topic)
        except Exception as exc:
            logger.warning("%s | unsubscribe failed for %s: %s", self.summary_log_prefix, symbol, exc)

        self.state.remove_subscribed_symbol(symbol)
        self.state.cleanup_orphan_price_state()

    def _make_ticker_handler(self, symbol: str):
        def handler(message: Dict[str, Any]) -> None:
            try:
                data = message.get("data", {})
                if isinstance(data, list) and data:
                    data = data[0]

                price = data.get("lastPrice") or data.get("markPrice") or data.get("indexPrice")
                price_float = self._to_float(price)
                if price_float is None:
                    return

                self.state.update_market_price(symbol, price_float)

            except Exception as exc:
                logger.warning("%s | ticker handler error for %s: %s", self.summary_log_prefix, symbol, exc)

        return handler

    def _on_wallet(self, message: Dict[str, Any]) -> None:
        try:
            data = message.get("data") or []
            if not data:
                return

            wallet = data[0]
            coins = {}
            for coin in wallet.get("coin", []):
                coin_name = coin.get("coin")
                if not coin_name:
                    continue
                coins[coin_name] = {
                    "walletBalance": self._to_float(coin.get("walletBalance")),
                    "equity": self._to_float(coin.get("equity")),
                    "usdValue": self._to_float(coin.get("usdValue")),
                    "unrealisedPnl": self._to_float(coin.get("unrealisedPnl")),
                }

            self.state.update_wallet(
                {
                    "available_balance": self._to_float(wallet.get("totalAvailableBalance")),
                    "wallet_balance": self._to_float(wallet.get("totalWalletBalance")),
                    "equity": self._to_float(wallet.get("totalEquity")),
                    "total_perp_upl_private": self._to_float(wallet.get("totalPerpUPL")),
                    "coins": coins,
                    "updated_time_exchange": message.get("creationTime"),
                }
            )
        except Exception as exc:
            logger.warning("%s | wallet handler error: %s", self.summary_log_prefix, exc)

    def _on_position(self, message: Dict[str, Any]) -> None:
        try:
            data = message.get("data") or []
            for pos in data:
                symbol = pos.get("symbol")
                if not symbol:
                    continue

                side = self._normalize_side(pos.get("side"))
                size = self._to_float(pos.get("size"))

                if side is None or size is None or size == 0:
                    self.state.remove_position(symbol)
                    continue

                self.state.update_position(
                    {
                        "symbol": symbol,
                        "side": side,
                        "size": size,
                        "entry_price": self._to_float(pos.get("entryPrice")),
                        "private_mark_price": self._to_float(pos.get("markPrice")),
                        "private_unrealized_pnl": self._to_float(pos.get("unrealisedPnl")),
                        "position_value": self._to_float(pos.get("positionValue")),
                        "liq_price": self._to_float(pos.get("liqPrice")),
                        "updated_time_exchange": pos.get("updatedTime"),
                    }
                )
        except Exception as exc:
            logger.warning("%s | position handler error: %s", self.summary_log_prefix, exc)

    def _on_order(self, message: Dict[str, Any]) -> None:
        try:
            data = message.get("data") or []
            for order in data:
                order_id = order.get("orderId")
                if not order_id:
                    continue

                self.state.update_order(
                    {
                        "order_id": order_id,
                        "symbol": order.get("symbol"),
                        "side": self._normalize_side(order.get("side")),
                        "order_type": order.get("orderType"),
                        "price": self._to_float(order.get("price")),
                        "qty": self._to_float(order.get("qty")),
                        "order_status": order.get("orderStatus"),
                        "cum_exec_qty": self._to_float(order.get("cumExecQty")),
                        "avg_price": self._to_float(order.get("avgPrice")),
                        "updated_time_exchange": order.get("updatedTime"),
                    }
                )
        except Exception as exc:
            logger.warning("%s | order handler error: %s", self.summary_log_prefix, exc)

    def _on_execution(self, message: Dict[str, Any]) -> None:
        try:
            data = message.get("data") or []
            for execution in data:
                self.state.add_execution(
                    {
                        "symbol": execution.get("symbol"),
                        "side": self._normalize_side(execution.get("side")),
                        "exec_price": self._to_float(execution.get("execPrice")),
                        "exec_qty": self._to_float(execution.get("execQty")),
                        "exec_fee": self._to_float(execution.get("execFee")),
                        "exec_pnl": self._to_float(execution.get("execPnl")),
                        "exec_time": execution.get("execTime"),
                        "order_id": execution.get("orderId"),
                        "exec_id": execution.get("execId"),
                    }
                )
        except Exception as exc:
            logger.warning("%s | execution handler error: %s", self.summary_log_prefix, exc)
            
    def _start_watchdog(self) -> None:
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop,
            name="support-watchdog",
            daemon=True,
        )
        self._watchdog_thread.start()
    
    def _watchdog_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.state.cleanup_orphan_price_state()

                health = self.state.get_health_snapshot(
                    private_stale_sec=60,
                    public_stale_sec=45,
                    public_symbol_grace_sec=30,
                )

                tracked_symbols = health.get("tracked_public_symbols", [])

                if health["private_stream_stale"] or (tracked_symbols and health["public_stream_stale"]):
                    logger.warning(
                        "%s | watchdog detected stale stream | private=%s public=%s stale_symbols=%s",
                        self.summary_log_prefix,
                        health["private_stream_stale"],
                        health["public_stream_stale"],
                        health["stale_symbols"],
                    )
                    self._restart_streams()

            except Exception as exc:
                logger.warning("%s | watchdog loop failed: %s", self.summary_log_prefix, exc)

            time.sleep(5.0)

    def _restart_streams(self) -> None:
        if not self._reconnect_lock.acquire(blocking=False):
            return

        try:
            if self._reconnect_in_progress:
                return

            now = time.time()
            if (now - self._last_restart_ts) < 15.0:
                return

            self._reconnect_in_progress = True
            logger.warning("%s | restarting support streams", self.summary_log_prefix)

            try:
                if self.private_ws is not None and hasattr(self.private_ws, "exit"):
                    self.private_ws.exit()
            except Exception as exc:
                logger.warning("%s | private ws restart-exit failed: %s", self.summary_log_prefix, exc)

            try:
                if self.public_ws is not None and hasattr(self.public_ws, "exit"):
                    self.public_ws.exit()
            except Exception as exc:
                logger.warning("%s | public ws restart-exit failed: %s", self.summary_log_prefix, exc)

            self.private_ws = None
            self.public_ws = None

            time.sleep(2.0)

            # Re-bootstrap current truth from REST, then rebuild streams
            self.bootstrap_rest()
            self._start_private_ws()
            self._start_public_ws()

            self._last_restart_ts = time.time()
            logger.warning("%s | support streams restarted", self.summary_log_prefix)

        finally:
            self._reconnect_in_progress = False
            self._reconnect_lock.release()
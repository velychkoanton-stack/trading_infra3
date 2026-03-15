from __future__ import annotations

import logging
import signal
import threading
import time
from typing import Optional

try:
    from .support_connection import SupportConnection
    from .support_state import SupportState
except ImportError:
    from support_connection import SupportConnection
    from support_state import SupportState


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


class SupportRunner:
    """
    Runtime wrapper for SupportLayerV1.
    """

    def __init__(
        self,
        environment: str = "demo",
        api_file_name: str = "api_credentials.txt",
        monitor: bool = True,
        monitor_interval_sec: int = 5,
    ) -> None:
        self.environment = environment
        self.api_file_name = api_file_name
        self.monitor = monitor
        self.monitor_interval_sec = monitor_interval_sec

        self.state = SupportState(environment=self.environment)
        self.connection = SupportConnection(
            state=self.state,
            environment=self.environment,
            api_file_name=self.api_file_name,
        )

        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        logger.info("SupportLayerV1 starting | environment=%s", self.environment)
        self.connection.start()

        if self.monitor:
            self._monitor_thread = threading.Thread(
                target=self._monitor_loop,
                name="support-monitor",
                daemon=True,
            )
            self._monitor_thread.start()

        logger.info("SupportLayerV1 started")

        while not self._stop_event.is_set():
            time.sleep(1.0)

    def stop(self) -> None:
        self._stop_event.set()
        self.connection.stop()
        logger.info("SupportLayerV1 stopped")

    def _monitor_loop(self) -> None:
        while not self._stop_event.is_set():
            self._print_monitor_snapshot()
            time.sleep(self.monitor_interval_sec)

    def _print_monitor_snapshot(self) -> None:
        account = self.state.get_account_snapshot()
        positions = self.state.get_positions_snapshot()
        orders = self.state.get_orders_snapshot()
        executions = self.state.get_recent_executions(limit=5)
        subscribed = sorted(self.state.get_subscribed_symbols())
        timestamps = self.state.get_timestamps_snapshot()
        health = self.state.get_health_snapshot()
        total_live_pnl = self.state.get_total_live_pnl()

        print("\n" + "=" * 100)
        print("SupportLayerV1 Monitor")
        print("=" * 100)

        print("\nACCOUNT SNAPSHOT")
        print(account)

        print("\nPOSITIONS")
        if not positions:
            print("No open positions")
        else:
            for symbol in sorted(positions.keys()):
                pos = positions[symbol]
                print(f"\n{symbol}")
                print(f"  side                  : {pos.get('side')}")
                print(f"  size                  : {pos.get('size')}")
                print(f"  entry_price           : {pos.get('entry_price')}")
                print(f"  private_mark_price    : {pos.get('private_mark_price')}")
                print(f"  private_unrealized_pnl: {pos.get('private_unrealized_pnl')}")
                print(f"  live_market_price     : {pos.get('live_market_price')}")
                print(f"  live_unrealized_pnl   : {pos.get('live_unrealized_pnl')}")
                print(f"  position_value        : {pos.get('position_value')}")
                print(f"  liq_price             : {pos.get('liq_price')}")
                print(f"  updated_time_exchange : {pos.get('updated_time_exchange')}")

        print(f"\nTOTAL LIVE UNREALIZED PNL: {total_live_pnl}")

        print("\nOPEN ORDERS")
        open_orders = {
            order_id: order
            for order_id, order in orders.items()
            if order.get("order_status") not in {"Filled", "Cancelled", "Rejected", "Deactivated"}
        }
        if not open_orders:
            print("No open orders")
        else:
            for order_id, order in open_orders.items():
                print(order_id, order)

        print("\nLAST EXECUTIONS")
        if not executions:
            print("No recent executions")
        else:
            for item in executions:
                print(item)

        print("\nACTIVE SUBSCRIBED SYMBOLS")
        print(subscribed)

        print("\nFRESHNESS / TIMESTAMPS")
        print(timestamps)

        print("\nHEALTH")
        print(health)

        print("=" * 100)


def main() -> None:
    runner = SupportRunner(
        environment="demo",
        api_file_name="api_credentials.txt",
        monitor=True,
        monitor_interval_sec=5,
    )

    def _shutdown_handler(signum, frame):  # noqa: ARG001
        runner.stop()

    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    runner.start()


if __name__ == "__main__":
    main()
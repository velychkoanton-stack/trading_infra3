from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from pathlib import Path

# Make project root importable when launched directly:
# python Execution_layer/bot_proto_1.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Execution_layer.Support_layer.support_runner import SupportRunner  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("bot_proto_1")


class BotProto1:
    """
    Minimal live test harness for SupportLayerV1.

    Purpose:
        - start SupportLayerV1
        - periodically read snapshots from state
        - verify live updates while you manually open/close demo positions

    This is NOT an executor bot.
    No DB.
    No strategy logic.
    No order placement.
    """

    def __init__(
        self,
        environment: str = "demo",
        api_file_name: str = "api_credentials.txt",
        support_monitor: bool = True,
        support_monitor_interval_sec: int = 5,
        proto_snapshot_interval_sec: int = 10,
    ) -> None:
        self.environment = environment
        self.api_file_name = api_file_name
        self.proto_snapshot_interval_sec = proto_snapshot_interval_sec

        self.runner = SupportRunner(
            environment=self.environment,
            api_file_name=self.api_file_name,
            monitor=support_monitor,
            monitor_interval_sec=support_monitor_interval_sec,
        )

        self._stop_event = threading.Event()
        self._proto_thread: threading.Thread | None = None
        self._runner_thread: threading.Thread | None = None

    def start(self) -> None:
        logger.info("bot_proto_1 starting | environment=%s", self.environment)

        self._runner_thread = threading.Thread(
            target=self.runner.start,
            name="support-runner-thread",
            daemon=True,
        )
        self._runner_thread.start()

        self._proto_thread = threading.Thread(
            target=self._proto_loop,
            name="bot-proto-1-loop",
            daemon=True,
        )
        self._proto_thread.start()

        while not self._stop_event.is_set():
            time.sleep(1.0)

    def stop(self) -> None:
        if self._stop_event.is_set():
            return

        logger.info("bot_proto_1 stopping")
        self._stop_event.set()
        self.runner.stop()
        logger.info("bot_proto_1 stopped")

    def _proto_loop(self) -> None:
        """
        Secondary observer loop.

        This is useful because:
        - support_runner monitor prints full state
        - this loop prints a compact service-level summary
        """
        while not self._stop_event.is_set():
            try:
                self._print_proto_summary()
            except Exception as exc:
                logger.exception("proto summary loop failed: %s", exc)

            time.sleep(self.proto_snapshot_interval_sec)

    def _print_proto_summary(self) -> None:
        state = self.runner.state

        account = state.get_account_snapshot()
        positions = state.get_positions_snapshot()
        subscribed = sorted(state.get_subscribed_symbols())
        health = state.get_health_snapshot(
            private_stale_sec=30,
            public_stale_sec=15,
        )
        total_live_pnl = state.get_total_live_pnl()

        print("\n" + "#" * 100)
        print("BOT_PROTO_1 SUMMARY")
        print("#" * 100)
        print(f"environment           : {account.get('environment')}")
        print(f"wallet_balance        : {account.get('wallet_balance')}")
        print(f"available_balance     : {account.get('available_balance')}")
        print(f"equity                : {account.get('equity')}")
        print(f"private_total_upl     : {account.get('total_perp_upl_private')}")
        print(f"open_positions_count  : {len(positions)}")
        print(f"subscribed_symbols    : {subscribed}")
        print(f"total_live_local_pnl  : {round(total_live_pnl, 8)}")
        print(f"private_stream_stale  : {health.get('private_stream_stale')}")
        print(f"public_stream_stale   : {health.get('public_stream_stale')}")
        print(f"stale_symbols         : {health.get('stale_symbols')}")

        if positions:
            print("\nOPEN POSITIONS COMPACT VIEW")
            for symbol in sorted(positions.keys()):
                pos = positions[symbol]
                print(
                    f"{symbol} | "
                    f"side={pos.get('side')} | "
                    f"size={pos.get('size')} | "
                    f"entry={pos.get('entry_price')} | "
                    f"live_px={pos.get('live_market_price')} | "
                    f"live_pnl={pos.get('live_unrealized_pnl')}"
                )

        print("#" * 100)


def main() -> None:
    bot = BotProto1(
        environment="demo",                 # change to "real" later when needed
        api_file_name="api_credentials.txt",
        support_monitor=True,               # full SupportLayer monitor
        support_monitor_interval_sec=5,
        proto_snapshot_interval_sec=10,     # compact summary
    )

    def _shutdown_handler(signum, frame):  # noqa: ARG001
        bot.stop()

    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    bot.start()


if __name__ == "__main__":
    main()
from __future__ import annotations

import threading
from typing import Any, Optional

from Execution_layer.Support_layer.support_runner import SupportRunner


class SupportBridge:
    """
    Thin production wrapper around SupportRunner.

    Purpose:
    - start/stop SupportLayerV1 in its own thread
    - expose clean snapshot getters to executor/control threads
    """

    def __init__(
        self,
        environment: str,
        api_file_name: str,
        monitor: bool = False,
        monitor_interval_sec: int = 10,
    ) -> None:
        self.environment = environment
        self.api_file_name = api_file_name
        self.monitor = monitor
        self.monitor_interval_sec = monitor_interval_sec

        self.runner = SupportRunner(
            environment=self.environment,
            api_file_name=self.api_file_name,
            monitor=self.monitor,
            monitor_interval_sec=self.monitor_interval_sec,
        )

        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._thread = threading.Thread(
            target=self.runner.start,
            name="support-bridge-thread",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self.runner.stop()

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def get_account_snapshot(self) -> dict[str, Any]:
        return self.runner.state.get_account_snapshot()

    def get_positions_snapshot(self) -> dict[str, dict[str, Any]]:
        return self.runner.state.get_positions_snapshot()

    def get_open_orders_snapshot(self) -> list[dict[str, Any]]:
        return self.runner.state.get_open_orders_snapshot()

    def get_recent_executions_snapshot(self) -> list[dict[str, Any]]:
        return self.runner.state.get_recent_executions_snapshot()

    def get_subscribed_symbols(self) -> set[str]:
        return self.runner.state.get_subscribed_symbols()

    def get_health_snapshot(
        self,
        private_stale_sec: int = 120,
        public_stale_sec: int = 120,
    ) -> dict[str, Any]:
        return self.runner.state.get_health_snapshot(
            private_stale_sec=private_stale_sec,
            public_stale_sec=public_stale_sec,
        )

    def get_total_live_pnl(self) -> float:
        return self.runner.state.get_total_live_pnl()

    def get_position(self, pybit_symbol: str) -> Optional[dict[str, Any]]:
        positions = self.get_positions_snapshot()
        return positions.get(pybit_symbol)

    def get_live_price(self, pybit_symbol: str) -> Optional[float]:
        position = self.get_position(pybit_symbol)
        if not position:
            return None
        return position.get("live_market_price")

    def get_live_unrealized_pnl(self, pybit_symbol: str) -> Optional[float]:
        position = self.get_position(pybit_symbol)
        if not position:
            return None
        return position.get("live_unrealized_pnl")

    def is_ws_healthy(self, max_stale_sec: int = 120) -> bool:
        health = self.get_health_snapshot(
            private_stale_sec=max_stale_sec,
            public_stale_sec=max_stale_sec,
        )
        return not bool(
            health.get("private_stream_stale", False)
            or health.get("public_stream_stale", False)
        )
from __future__ import annotations

import signal
from pathlib import Path

from Execution_layer.Executors.executor_runtime import ExecutorRuntime
from Execution_layer.Executors.models import ExecutorBotConfig


def build_config() -> ExecutorBotConfig:

    rules_path = Path(__file__).resolve().parent / "rules" / "rules.txt"

    return ExecutorBotConfig(
        bot_id="bot_L1_01",
        environment="demo",

        mysql_api_file="api_mysql_main.txt",
        bybit_api_file="api_credentials_demo_1.txt",
        telegram_api_file="api_telegram_level_1.txt",

        rules_file_path=str(rules_path),

        support_monitor=False,
        support_monitor_interval_sec=10,

        heartbeat_interval_sec=30,
        worker_loop_sec=30,

        executor_threads_count=2,

        executor_start_delay_sec=20.0,

        ws_stale_close_sec=120,
        signal_stale_sec=600,
        pair_state_stale_sec=1800,
    )


def main():

    runtime = ExecutorRuntime(build_config())
    runtime.build()

    def shutdown_handler(signum, frame):  # noqa
        runtime.stop()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    runtime.start()


if __name__ == "__main__":
    main()
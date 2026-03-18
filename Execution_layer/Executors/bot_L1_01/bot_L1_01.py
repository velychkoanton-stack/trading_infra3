from __future__ import annotations

import signal
from pathlib import Path

from Common.config.rules_loader import load_rules_file
from Execution_layer.Executors.executor_runtime import ExecutorRuntime
from Execution_layer.Executors.models import ExecutorBotConfig


def _rule_str(rules: dict[str, str], key: str, default: str) -> str:
    return str(rules.get(key, default)).strip()


def _rule_int(rules: dict[str, str], key: str, default: int) -> int:
    try:
        return int(float(rules.get(key, default)))
    except Exception:
        return int(default)


def _rule_float(rules: dict[str, str], key: str, default: float) -> float:
    try:
        return float(rules.get(key, default))
    except Exception:
        return float(default)


def _rule_bool(rules: dict[str, str], key: str, default: bool) -> bool:
    raw = str(rules.get(key, str(default))).strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def build_config() -> ExecutorBotConfig:
    rules_path = Path(__file__).resolve().parent / "rules" / "rules.txt"
    rules = load_rules_file(str(rules_path))

    return ExecutorBotConfig(
        bot_id=_rule_str(rules, "bot_id", "bot_L1_01"),
        environment=_rule_str(rules, "environment", "demo"),

        mysql_api_file=_rule_str(rules, "mysql_api_file", "api_mysql_main.txt"),
        bybit_api_file=_rule_str(rules, "bybit_api_file", "api_credentials_demo_1.txt"),
        telegram_api_file=_rule_str(rules, "telegram_api_file", "api_telegram_level_1.txt"),

        rules_file_path=str(rules_path),

        support_monitor=_rule_bool(rules, "support_monitor", False),
        support_monitor_interval_sec=_rule_int(rules, "support_monitor_interval_sec", 10),

        heartbeat_interval_sec=_rule_int(rules, "heartbeat_interval_sec", 30),
        worker_loop_sec=_rule_int(rules, "worker_loop_sec", 30),

        executor_threads_count=_rule_int(rules, "executor_threads_count", 2),
        executor_start_delay_sec=_rule_float(rules, "executor_start_delay_sec", 20.0),

        ws_stale_close_sec=_rule_int(rules, "ws_stale_close_sec", 120),
        signal_stale_sec=_rule_int(rules, "signal_stale_sec", 600),
        pair_state_stale_sec=_rule_int(rules, "pair_state_stale_sec", 1800),
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
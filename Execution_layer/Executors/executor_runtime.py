from __future__ import annotations

from pathlib import Path

from Common.config.rules_loader import load_rules_file
from Common.utils.logger import setup_logger

from Execution_layer.Executors.control_worker import ControlWorker
from Execution_layer.Executors.executor_base import ExecutorBase
from Execution_layer.Executors.executor_worker import ExecutorWorker
from Execution_layer.Executors.order_manager import OrderManager
from Execution_layer.Executors.repositories import ExecutorRepositories
from Execution_layer.Executors.shared_state import SharedExecutorState
from Execution_layer.Support_layer.support_bridge import SupportBridge


class ExecutorRuntime:
    """
    One self-contained bot runtime.

    Contains:
    - SupportBridge thread
    - ControlWorker thread
    - N ExecutorWorker threads

    This runtime is designed to be started manually from one bot entrypoint.
    """

    def __init__(self, bot_config) -> None:
        self.bot_config = bot_config

        self.rules: dict[str, str] = {}
        self.logger = None

        self.support_bridge: SupportBridge | None = None
        self.shared_state: SharedExecutorState | None = None
        self.repositories: ExecutorRepositories | None = None
        self.order_manager: OrderManager | None = None
        self.control_worker: ControlWorker | None = None
        self.executor_workers: list[ExecutorWorker] = []

        self._built = False

    # -------------------------------------------------------
    # BUILD
    # -------------------------------------------------------

    def build(self) -> None:
        if self._built:
            return

        self.rules = load_rules_file(self.bot_config.rules_file_path)

        bot_dir = Path(self.bot_config.rules_file_path).resolve().parents[1]
        log_dir = bot_dir / "logs"
        log_file = log_dir / f"{self.bot_config.bot_id}.log"

        self.logger = setup_logger(
            logger_name=self.bot_config.bot_id,
            log_file_path=log_file,
        )

        sql_dir = Path(__file__).resolve().parent / self.bot_config.sql_dir_name

        self.support_bridge = SupportBridge(
            environment=self.bot_config.environment,
            api_file_name=self.bot_config.bybit_api_file,
            monitor=self.bot_config.support_monitor,
            monitor_interval_sec=self.bot_config.support_monitor_interval_sec,
        )

        self.shared_state = SharedExecutorState()

        self.repositories = ExecutorRepositories(
            api_file_name=self.bot_config.mysql_api_file,
            sql_dir=sql_dir,
            asset_lock_table=self.bot_config.asset_lock_table,
        )

        self.order_manager = OrderManager(
            api_file_name=self.bot_config.bybit_api_file,
            environment=self.bot_config.environment,
            logger=self.logger,
            rules=self.rules,
        )

        self.control_worker = ControlWorker(
            bot_config=self.bot_config,
            support_bridge=self.support_bridge,
            shared_state=self.shared_state,
            repositories=self.repositories,
            logger=self.logger,
        )

        self.executor_workers = []

        for idx in range(1, self.bot_config.executor_threads_count + 1):
            worker_id = f"{self.bot_config.bot_id}_exec_{idx:02d}"

            executor_base = ExecutorBase(
                bot_config=self.bot_config,
                worker_id=worker_id,
                rules=self.rules,
                support_bridge=self.support_bridge,
                shared_state=self.shared_state,
                repositories=self.repositories,
                order_manager=self.order_manager,
                logger=self.logger,
            )

            worker = ExecutorWorker(
                executor_base=executor_base,
                repositories=self.repositories,
                bot_id=self.bot_config.bot_id,
                worker_id=worker_id,
                startup_delay_sec=self.bot_config.executor_start_delay_sec * (idx - 1),
                logger=self.logger,
            )

            self.executor_workers.append(worker)

        self._built = True
        self.logger.info("ExecutorRuntime built bot_id=%s", self.bot_config.bot_id)

    # -------------------------------------------------------
    # START / STOP
    # -------------------------------------------------------

    def start(self) -> None:
        if not self._built:
            raise RuntimeError("ExecutorRuntime.build() must be called before start()")

        assert self.logger is not None
        assert self.support_bridge is not None
        assert self.control_worker is not None

        self.logger.info("ExecutorRuntime starting bot_id=%s", self.bot_config.bot_id)

        self.support_bridge.start()
        self.control_worker.start()

        for worker in self.executor_workers:
            worker.start()

        self.logger.info(
            "ExecutorRuntime started bot_id=%s executor_threads=%s",
            self.bot_config.bot_id,
            len(self.executor_workers),
        )

        try:
            while True:
                # keep main thread alive
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self) -> None:
        if not self._built:
            return

        assert self.logger is not None

        self.logger.info("ExecutorRuntime stopping bot_id=%s", self.bot_config.bot_id)

        for worker in self.executor_workers:
            try:
                worker.stop()
            except Exception:
                self.logger.exception("Failed to stop worker")

        if self.control_worker is not None:
            try:
                self.control_worker.stop()
            except Exception:
                self.logger.exception("Failed to stop control worker")

        if self.support_bridge is not None:
            try:
                self.support_bridge.stop()
            except Exception:
                self.logger.exception("Failed to stop support bridge")

        self.logger.info("ExecutorRuntime stopped bot_id=%s", self.bot_config.bot_id)

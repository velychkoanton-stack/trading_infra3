from __future__ import annotations

import threading
import time
from datetime import date, datetime

from Common.db.heartbeat_writer import write_heartbeat
from Execution_layer.Executors.models import PairLiveMetrics


class ControlWorker:
    """
    Thread #2 in executor runtime.

    Responsibilities:
    - write bot heartbeat every cycle
    - ensure/update bot_daily_snapshot
    - calculate pair unrealized pnl from shared open-pair registry + SupportBridge
    - maintain bot_position_value
    - detect stale WS and set shared ws_critical flag
    """

    def __init__(
        self,
        bot_config,
        support_bridge,
        shared_state,
        repositories,
        logger,
    ) -> None:
        self.bot_config = bot_config
        self.support_bridge = support_bridge
        self.shared_state = shared_state
        self.repositories = repositories
        self.logger = logger

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # -------------------------------------------------------
    # LIFECYCLE
    # -------------------------------------------------------

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._thread = threading.Thread(
            target=self.run_loop,
            name=f"{self.bot_config.bot_id}-control-worker",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    # -------------------------------------------------------
    # MAIN LOOP
    # -------------------------------------------------------

    def run_loop(self) -> None:
        self.logger.info("ControlWorker started bot_id=%s", self.bot_config.bot_id)

        while not self._stop_event.is_set():
            try:
                self.run_cycle()
            except Exception:
                self.logger.exception("ControlWorker cycle failed")
                self.write_heartbeat(runtime_status="ERROR", comment="control_worker_cycle_failed")

            time.sleep(self.bot_config.heartbeat_interval_sec)

        self.write_heartbeat(runtime_status="STOPPED", comment="control_worker_stopped")
        self.logger.info("ControlWorker stopped bot_id=%s", self.bot_config.bot_id)

    def run_cycle(self) -> None:
        self.write_heartbeat(runtime_status="RUNNING", comment="control_worker_ok")
        self.ensure_daily_snapshot()
        self.update_pair_metrics_and_position_values()
        self.update_ws_critical_flag()

    # -------------------------------------------------------
    # HEARTBEAT
    # -------------------------------------------------------

    def write_heartbeat(self, runtime_status: str, comment: str | None = None) -> None:
        write_heartbeat(
            worker_id=self.bot_config.bot_id,
            runtime_status=runtime_status,
            comment=comment,
            api_file_name=self.bot_config.mysql_api_file,
        )

    # -------------------------------------------------------
    # DAILY SNAPSHOT
    # -------------------------------------------------------

    def ensure_daily_snapshot(self) -> None:
        account = self.support_bridge.get_account_snapshot()

        equity = float(account.get("equity") or 0.0)
        balance = float(account.get("wallet_balance") or 0.0)

        today = date.today()
        now = datetime.now()

        self.repositories.ensure_daily_snapshot(
            bot_id=self.bot_config.bot_id,
            snapshot_date=today,
            start_equity=equity,
            start_balance=balance,
            current_equity=equity,
            start_ts=now,
        )

        self.repositories.update_current_equity(
            bot_id=self.bot_config.bot_id,
            snapshot_date=today,
            current_equity=equity,
        )

    # -------------------------------------------------------
    # PAIR METRICS / POSITION VALUE
    # -------------------------------------------------------

    def update_pair_metrics_and_position_values(self) -> None:
        open_pairs = self.shared_state.get_open_pairs_for_bot(self.bot_config.bot_id)

        for record in open_pairs:
            metrics = self._build_pair_metrics(record)

            if metrics is None:
                continue

            self.shared_state.update_pair_metrics(metrics)

            self.repositories.upsert_position_value(
                bot_id=self.bot_config.bot_id,
                uuid=record.uuid,
                pos_value=metrics.current_pos_value,
                unrealized_pnl=metrics.unrealized_pnl,
                updated_at=metrics.updated_at,
            )

    def _build_pair_metrics(self, record) -> PairLiveMetrics | None:
        pos1 = self.support_bridge.get_position(record.pybit_symbol_1)
        pos2 = self.support_bridge.get_position(record.pybit_symbol_2)

        pnl1 = 0.0
        pnl2 = 0.0
        pos_val_1 = 0.0
        pos_val_2 = 0.0

        if pos1:
            pnl1 = float(pos1.get("live_unrealized_pnl") or 0.0)
            pos_val_1 = float(pos1.get("position_value") or 0.0)

        if pos2:
            pnl2 = float(pos2.get("live_unrealized_pnl") or 0.0)
            pos_val_2 = float(pos2.get("position_value") or 0.0)

        pair_unrealized_pnl = pnl1 + pnl2
        current_leg_sum = pos_val_1 + pos_val_2
        current_pos_value = max(current_leg_sum, float(record.initial_exposure))

        return PairLiveMetrics(
            uuid=record.uuid,
            unrealized_pnl=pair_unrealized_pnl,
            current_pos_value=current_pos_value,
            updated_at=datetime.now(),
        )

    # -------------------------------------------------------
    # WS HEALTH
    # -------------------------------------------------------

    def update_ws_critical_flag(self) -> None:
        health = self.support_bridge.get_health_snapshot(
            private_stale_sec=self.bot_config.ws_stale_close_sec,
            public_stale_sec=self.bot_config.ws_stale_close_sec,
        )

        now = datetime.now().timestamp()

        private_ws_started = health.get("private_ws_started")
        public_ws_started = health.get("public_ws_started")
        rest_bootstrap = health.get("rest_bootstrap")

        startup_grace_sec = 60

        def _ms_to_age_sec(ts_ms):
            if not ts_ms:
                return None
            return now - (float(ts_ms) / 1000.0)

        private_ws_age = _ms_to_age_sec(private_ws_started)
        public_ws_age = _ms_to_age_sec(public_ws_started)
        rest_age = _ms_to_age_sec(rest_bootstrap)

        in_startup_grace = (
            rest_bootstrap is None
            or private_ws_started is None
            or public_ws_started is None
            or (rest_age is not None and rest_age < startup_grace_sec)
            or (private_ws_age is not None and private_ws_age < startup_grace_sec)
            or (public_ws_age is not None and public_ws_age < startup_grace_sec)
        )

        if in_startup_grace:
            self.shared_state.set_ws_critical(False, comment=None)
            self.logger.info(
                "ControlWorker ws health in startup grace | bot_id=%s",
                self.bot_config.bot_id,
            )
            return

        private_stale = bool(health.get("private_stream_stale", False))
        public_stale = bool(health.get("public_stream_stale", False))

        open_pairs = self.shared_state.get_open_pairs_for_bot(self.bot_config.bot_id)
        has_open_pairs = len(open_pairs) > 0

        # Private stale matters only when we are actively managing live trades.
        # When bot is idle, private stream may naturally be quiet.
        private_is_critical = private_stale and has_open_pairs
        public_is_critical = public_stale

        is_critical = private_is_critical or public_is_critical

        if is_critical:
            self.shared_state.set_ws_critical(
                True,
                comment=(
                    f"ws_stale private={private_stale} public={public_stale} "
                    f"has_open_pairs={has_open_pairs}"
                ),
            )
        else:
            self.shared_state.set_ws_critical(False, comment=None)
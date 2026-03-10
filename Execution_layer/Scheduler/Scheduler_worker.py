from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from Common.db.db_execute import fetch_all, execute
from Common.db.heartbeat_writer import write_heartbeat
from Common.utils.cleanup import force_gc
from Common.utils.logger import setup_logger
from Common.utils.sql_file_loader import load_sql_file


VALID_CONTROL_STATUSES = {"RUNNING", "SLEEP", "SL_BLOCK", "STOP"}


@dataclass(frozen=True)
class WeekendWindow:
    start_weekday: int  # Monday=0 ... Sunday=6
    start_hour: int
    start_minute: int
    end_weekday: int
    end_hour: int
    end_minute: int


class SchedulerWorker:
    def __init__(self) -> None:
        self.scheduler_dir = Path(__file__).resolve().parent
        self.project_root = self.scheduler_dir.parents[1]

        self.rules_path = self.scheduler_dir / "rules" / "rules.txt"
        self.sql_dir = self.scheduler_dir / "sql_queries"
        self.log_path = self.project_root / "data" / "logs" / "Execution_layer" / "Scheduler" / "scheduler.log"

        self.logger = setup_logger(
            logger_name="ExecutionLayer.Scheduler",
            log_file_path=self.log_path,
        )

        self.rules = self._load_rules_file(self.rules_path)
        self.mysql_api_file = self.rules.get("mysql_api_file", "api_mysql_main.txt")
        self.loop_sec = self._parse_positive_int(self.rules.get("scheduler_loop_sec", "60"), "scheduler_loop_sec")
        self.update_only_on_change = self._parse_bool(self.rules.get("update_only_on_change", "1"))
        self.timezone_name = self.rules.get("timezone", "Europe/Amsterdam")
        self.tz = ZoneInfo(self.timezone_name)
        self.weekend_window = self._parse_weekend_window(
            self.rules["weekend_block_start"],
            self.rules["weekend_block_end"],
        )
        self.worker_matrix = self._build_worker_matrix(self.rules)

        self.select_scheduler_all_sql = load_sql_file(self.sql_dir / "select_scheduler_all.txt")
        self.upsert_scheduler_row_sql = load_sql_file(self.sql_dir / "upsert_scheduler_row.txt")

    def run_forever(self) -> None:
        self.logger.info("Scheduler worker started. loop_sec=%s timezone=%s", self.loop_sec, self.timezone_name)

        self._safe_write_heartbeat(
            runtime_status="RUNNING",
            comment="scheduler_started",
        )

        while True:
            loop_started = time.time()

            try:
                self.run_once()

                self._safe_write_heartbeat(
                    runtime_status="RUNNING",
                    comment="loop_ok",
                )

            except Exception as exc:
                self.logger.exception("Scheduler loop failed")

                self._safe_write_heartbeat(
                    runtime_status="ERROR",
                    comment=self._truncate_comment(f"loop_error:{type(exc).__name__}"),
                )

            elapsed = time.time() - loop_started
            sleep_seconds = max(1.0, self.loop_sec - elapsed)
            time.sleep(sleep_seconds)
            force_gc()

    def run_once(self) -> None:
        now_local = datetime.now(self.tz)
        now_naive = now_local.replace(tzinfo=None)

        existing_rows = self._fetch_existing_scheduler_rows()
        desired_rows = self._build_desired_scheduler_rows(now_local, now_naive)

        changed_count = 0

        for worker_id, desired_row in desired_rows.items():
            current_row = existing_rows.get(worker_id)

            should_write = True
            if self.update_only_on_change and current_row is not None:
                should_write = not self._rows_equal(current_row, desired_row)

            if should_write:
                execute(
                    sql=self.upsert_scheduler_row_sql,
                    api_file_name=self.mysql_api_file,
                    params=desired_row,
                )
                changed_count += 1
                self.logger.info(
                    "scheduler updated | worker_id=%s | control_status=%s | comment=%s",
                    desired_row["worker_id"],
                    desired_row["control_status"],
                    desired_row["comment"],
                )

        self.logger.info(
            "scheduler loop complete | workers_total=%s | rows_changed=%s | now=%s",
            len(desired_rows),
            changed_count,
            now_local.strftime("%Y-%m-%d %H:%M:%S %Z"),
        )

    def _safe_write_heartbeat(self, runtime_status: str, comment: str | None) -> None:
        """
        Heartbeat must never break the scheduler loop.
        """
        try:
            write_heartbeat(
                worker_id="scheduler",
                runtime_status=runtime_status,
                comment=comment,
                api_file_name=self.mysql_api_file,
            )
        except Exception:
            self.logger.exception(
                "Failed to write scheduler heartbeat | runtime_status=%s | comment=%s",
                runtime_status,
                comment,
            )

    def _fetch_existing_scheduler_rows(self) -> dict[str, dict[str, Any]]:
        rows = fetch_all(
            sql=self.select_scheduler_all_sql,
            api_file_name=self.mysql_api_file,
        )
        return {row["worker_id"]: row for row in rows}

    def _build_desired_scheduler_rows(
        self,
        now_local: datetime,
        now_naive: datetime,
    ) -> dict[str, dict[str, Any]]:
        desired: dict[str, dict[str, Any]] = {}
        is_weekend_block = self._is_within_weekend_window(now_local, self.weekend_window)

        for worker_id, worker_rules in self.worker_matrix.items():
            # GLOBAL is manual/top-level control and is not auto-modified by scheduler.
            if worker_id == "GLOBAL":
                continue

            control_status, comment = self._resolve_control_state(
                worker_id=worker_id,
                worker_rules=worker_rules,
                is_weekend_block=is_weekend_block,
            )

            desired[worker_id] = {
                "worker_id": worker_id,
                "control_status": control_status,
                "last_update_ts": now_naive,
                "comment": comment,
            }

        return desired

    def _resolve_control_state(
        self,
        worker_id: str,
        worker_rules: dict[str, str],
        is_weekend_block: bool,
    ) -> tuple[str, str]:
        enabled = self._parse_bool(worker_rules.get("enabled", "1"))
        default_status = worker_rules.get("default_status", "RUNNING").strip().upper()
        weekend_mode = worker_rules.get("weekend_mode", default_status).strip().upper()

        self._validate_control_status(default_status, worker_id, "default_status")
        self._validate_control_status(weekend_mode, worker_id, "weekend_mode")

        if not enabled:
            return "STOP", "disabled_in_rules"

        # Daily SL is intentionally not applied yet because bot_daily_snapshot schema
        # and query contract have not been finalized in this step.
        # worker_rules["daily_sl_pct"] is already kept in rules.txt for the next stage.

        if is_weekend_block:
            return weekend_mode, "weekend_schedule"

        return default_status, "default_status"

    @staticmethod
    def _rows_equal(current_row: dict[str, Any], desired_row: dict[str, Any]) -> bool:
        return (
            str(current_row.get("control_status", "")).upper() == str(desired_row["control_status"]).upper()
            and (current_row.get("comment") or "") == (desired_row["comment"] or "")
        )

    @staticmethod
    def _truncate_comment(comment: str | None, max_len: int = 64) -> str | None:
        if comment is None:
            return None
        return str(comment)[:max_len]

    @staticmethod
    def _load_rules_file(file_path: Path) -> dict[str, str]:
        if not file_path.exists():
            raise FileNotFoundError(f"Rules file not found: {file_path}")

        rules: dict[str, str] = {}

        for raw_line in file_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()

            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                raise ValueError(f"Invalid rules line (missing '='): {line}")

            key, value = line.split("=", 1)
            rules[key.strip()] = value.strip()

        if not rules:
            raise ValueError(f"Rules file is empty: {file_path}")

        return rules

    @staticmethod
    def _build_worker_matrix(rules: dict[str, str]) -> dict[str, dict[str, str]]:
        matrix: dict[str, dict[str, str]] = {}

        for key, value in rules.items():
            if not key.startswith("worker."):
                continue

            parts = key.split(".")
            if len(parts) != 3:
                raise ValueError(f"Invalid worker rule key format: {key}")

            _, worker_id, field_name = parts
            matrix.setdefault(worker_id, {})
            matrix[worker_id][field_name] = value

        if not matrix:
            raise ValueError("No worker.* rules found in rules.txt")

        if "GLOBAL" not in matrix:
            raise ValueError("Missing required worker.GLOBAL.* rules")

        if "scheduler" not in matrix:
            raise ValueError("Missing required worker.scheduler.* rules")

        return matrix

    @staticmethod
    def _parse_bool(value: str) -> bool:
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "yes", "y"}:
            return True
        if normalized in {"0", "false", "no", "n"}:
            return False
        raise ValueError(f"Invalid boolean value: {value}")

    @staticmethod
    def _parse_positive_int(value: str, field_name: str) -> int:
        parsed = int(str(value).strip())
        if parsed <= 0:
            raise ValueError(f"{field_name} must be > 0, got {value}")
        return parsed

    @staticmethod
    def _validate_control_status(status: str, worker_id: str, field_name: str) -> None:
        if status not in VALID_CONTROL_STATUSES:
            raise ValueError(
                f"Invalid control status for worker_id={worker_id}, field={field_name}: {status}. "
                f"Allowed={sorted(VALID_CONTROL_STATUSES)}"
            )

    @staticmethod
    def _parse_weekend_window(start_value: str, end_value: str) -> WeekendWindow:
        start_weekday, start_hour, start_minute = SchedulerWorker._parse_day_time(start_value)
        end_weekday, end_hour, end_minute = SchedulerWorker._parse_day_time(end_value)

        return WeekendWindow(
            start_weekday=start_weekday,
            start_hour=start_hour,
            start_minute=start_minute,
            end_weekday=end_weekday,
            end_hour=end_hour,
            end_minute=end_minute,
        )

    @staticmethod
    def _parse_day_time(value: str) -> tuple[int, int, int]:
        day_map = {
            "MON": 0,
            "TUE": 1,
            "WED": 2,
            "THU": 3,
            "FRI": 4,
            "SAT": 5,
            "SUN": 6,
        }

        parts = value.strip().upper().split()
        if len(parts) != 2:
            raise ValueError(f"Invalid day-time format: {value}. Expected e.g. 'FRI 15:00'")

        day_part, time_part = parts
        if day_part not in day_map:
            raise ValueError(f"Invalid weekday in rules: {day_part}")

        hour_str, minute_str = time_part.split(":")
        hour = int(hour_str)
        minute = int(minute_str)

        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(f"Invalid time in rules: {time_part}")

        return day_map[day_part], hour, minute

    @staticmethod
    def _is_within_weekend_window(now_local: datetime, weekend_window: WeekendWindow) -> bool:
        current_minutes = now_local.weekday() * 1440 + now_local.hour * 60 + now_local.minute
        start_minutes = (
            weekend_window.start_weekday * 1440
            + weekend_window.start_hour * 60
            + weekend_window.start_minute
        )
        end_minutes = (
            weekend_window.end_weekday * 1440
            + weekend_window.end_hour * 60
            + weekend_window.end_minute
        )

        if start_minutes <= end_minutes:
            return start_minutes <= current_minutes < end_minutes

        return current_minutes >= start_minutes or current_minutes < end_minutes


if __name__ == "__main__":
    worker = SchedulerWorker()
    worker.run_forever()
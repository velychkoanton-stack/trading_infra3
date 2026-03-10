from __future__ import annotations

from datetime import datetime
from pathlib import Path

from Common.db.db_execute import execute
from Common.utils.sql_file_loader import load_sql_file


VALID_RUNTIME_STATUSES = {"RUNNING", "SLEEPING", "ERROR", "STOPPED"}


def _get_project_root() -> Path:
    """
    Resolve Trading_infra project root from:
    Common/db/heartbeat_writer.py
    """
    return Path(__file__).resolve().parents[2]


def _get_default_sql_path() -> Path:
    """
    Default heartbeat UPSERT SQL location.
    """
    return _get_project_root() / "Execution_layer" / "Bot_heartbeat" / "sql_queries" / "upsert_bot_heartbeat.txt"


def write_heartbeat(
    worker_id: str,
    runtime_status: str,
    comment: str | None,
    api_file_name: str,
    sql_file_path: str | Path | None = None,
) -> int:
    """
    Insert or update one live heartbeat row for a worker.

    Notes:
    - one row per worker_id
    - no history table
    - timestamp is written as naive datetime, matching current project DB style
    """
    normalized_worker_id = str(worker_id).strip()
    normalized_status = str(runtime_status).strip().upper()
    normalized_comment = None if comment is None else str(comment).strip()

    if not normalized_worker_id:
        raise ValueError("worker_id must not be empty")

    if normalized_status not in VALID_RUNTIME_STATUSES:
        raise ValueError(
            f"Invalid runtime_status={runtime_status}. "
            f"Allowed={sorted(VALID_RUNTIME_STATUSES)}"
        )

    sql_path = Path(sql_file_path) if sql_file_path else _get_default_sql_path()
    sql = load_sql_file(sql_path)

    params = {
        "worker_id": normalized_worker_id,
        "runtime_status": normalized_status,
        "last_update_ts": datetime.now(),
        "comment": normalized_comment,
    }

    return execute(
        sql=sql,
        api_file_name=api_file_name,
        params=params,
    )
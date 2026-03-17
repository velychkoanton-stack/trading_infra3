from __future__ import annotations

from contextlib import closing
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from Common.db.db_execute import execute, fetch_all, fetch_one
from Common.db.db_transaction import run_in_transaction
from Common.utils.sql_file_loader import load_sql_file

from Execution_layer.Executors.models import CandidatePair


class ExecutorRepositories:
    """
    Central DB gateway for executor layer.

    All SQL access from executors goes through this class.
    """

    def __init__(self, api_file_name: str, sql_dir: str | Path):
        self.api_file_name = api_file_name
        self.sql_dir = Path(sql_dir)

        # load SQL files once
        self.sql_select_candidates = load_sql_file(self.sql_dir / "select_candidates_base.txt")
        self.sql_select_candidate_by_uuid = load_sql_file(self.sql_dir / "select_candidate_by_uuid.txt")

        self.sql_insert_asset_lock = load_sql_file(self.sql_dir / "insert_asset_lock.txt")
        self.sql_delete_asset_locks = load_sql_file(self.sql_dir / "delete_asset_locks_by_uuid_bot.txt")

        self.sql_insert_trade_open = load_sql_file(self.sql_dir / "insert_trade_open.txt")
        self.sql_update_trade_close = load_sql_file(self.sql_dir / "update_trade_close.txt")

        self.sql_select_scheduler_status = load_sql_file(self.sql_dir / "select_scheduler_status.txt")

        self.sql_upsert_daily_snapshot = load_sql_file(self.sql_dir / "upsert_daily_snapshot.txt")
        self.sql_update_daily_equity = load_sql_file(self.sql_dir / "update_daily_snapshot_equity.txt")

        self.sql_upsert_position_value = load_sql_file(self.sql_dir / "upsert_bot_position_value.txt")
        self.sql_delete_position_value = load_sql_file(self.sql_dir / "delete_bot_position_value.txt")

    # -------------------------------------------------
    # SIGNAL / PAIR STATE
    # -------------------------------------------------

    def fetch_candidate_pool(
        self,
        level_180: str,
        z_upper_threshold: float,
        num_trades_180_min: int,
        num_trades_180_max: int,
    ) -> list[CandidatePair]:
        rows = fetch_all(
            sql=self.sql_select_candidates,
            api_file_name=self.api_file_name,
            params={
                "level_180": level_180,
                "z_upper_threshold": z_upper_threshold,
                "num_trades_180_min": num_trades_180_min,
                "num_trades_180_max": num_trades_180_max,
            },
        )

        result: list[CandidatePair] = []

        for row in rows:
            result.append(CandidatePair(**row))

        return result

    # -------------------------------------------------
    # ASSET LOCKS
    # -------------------------------------------------

    def try_lock_pair_assets(
        self,
        bot_id: str,
        uuid: str,
        asset_1: str,
        asset_2: str,
    ) -> bool:
        """
        Atomically lock both assets for one pair.

        Returns:
        - True if both assets were successfully locked
        - False if one or both assets are already locked
        """

        def _operation(conn) -> bool:
            with closing(conn.cursor(dictionary=True)) as cursor:
                # Re-check inside transaction
                cursor.execute(
                    """
                    SELECT asset
                    FROM asset_locks
                    WHERE asset IN (%s, %s)
                    LIMIT 1
                    """,
                    (asset_1, asset_2),
                )
                existing = cursor.fetchone()

                if existing is not None:
                    return False

                cursor.execute(
                    self.sql_insert_asset_lock,
                    {
                        "bot_id": bot_id,
                        "uuid": uuid,
                        "asset": asset_1,
                    },
                )

                cursor.execute(
                    self.sql_insert_asset_lock,
                    {
                        "bot_id": bot_id,
                        "uuid": uuid,
                        "asset": asset_2,
                    },
                )

                return True

        return run_in_transaction(
            api_file_name=self.api_file_name,
            operation=_operation,
        )



    def insert_asset_lock(self, bot_id: str, uuid: str, asset: str) -> int:
        return execute(
            sql=self.sql_insert_asset_lock,
            api_file_name=self.api_file_name,
            params={
                "bot_id": bot_id,
                "uuid": uuid,
                "asset": asset,
            },
        )

    def delete_asset_locks(self, bot_id: str, uuid: str) -> int:
        return execute(
            sql=self.sql_delete_asset_locks,
            api_file_name=self.api_file_name,
            params={
                "bot_id": bot_id,
                "uuid": uuid,
            },
        )

    # -------------------------------------------------
    # TRADE RESULTS
    # -------------------------------------------------

    def insert_trade_open(
        self,
        uuid: str,
        bot_id: str,
        pos_val: float,
        open_cond: str | None,
    ) -> int:
        """
        Insert trade_res open row and return the actual auto-increment ID.
        """

        def _operation(conn) -> int:
            with closing(conn.cursor()) as cursor:
                cursor.execute(
                    self.sql_insert_trade_open,
                    {
                        "uuid": uuid,
                        "bot_id": bot_id,
                        "pos_val": pos_val,
                        "open_cond": open_cond,
                    },
                )
                return int(cursor.lastrowid or 0)

        return run_in_transaction(
            api_file_name=self.api_file_name,
            operation=_operation,
        )

    def update_trade_close(
        self,
        trade_id: int,
        pnl: float,
        pnl_pers: float,
        closed_by: str,
        close_cond: str | None,
    ) -> int:
        return execute(
            sql=self.sql_update_trade_close,
            api_file_name=self.api_file_name,
            params={
                "trade_id": trade_id,
                "pnl": pnl,
                "pnl_pers": pnl_pers,
                "closed_by": closed_by,
                "close_cond": close_cond,
            },
        )

    # -------------------------------------------------
    # SCHEDULER
    # -------------------------------------------------

    def get_scheduler_status(self, worker_id: str) -> str:
        row = fetch_one(
            sql=self.sql_select_scheduler_status,
            api_file_name=self.api_file_name,
            params=(worker_id,),
        )

        if not row:
            return "RUNNING"

        return row["control_status"]

    # -------------------------------------------------
    # DAILY SNAPSHOT
    # -------------------------------------------------

    def ensure_daily_snapshot(
        self,
        bot_id: str,
        snapshot_date: date,
        start_equity: float,
        start_balance: float,
        current_equity: float,
        start_ts: datetime,
    ) -> int:
        return execute(
            sql=self.sql_upsert_daily_snapshot,
            api_file_name=self.api_file_name,
            params={
                "bot_id": bot_id,
                "snapshot_date": snapshot_date,
                "start_equity": start_equity,
                "start_balance": start_balance,
                "current_equity": current_equity,
                "start_ts": start_ts,
            },
        )

    def update_current_equity(
        self,
        bot_id: str,
        snapshot_date: date,
        current_equity: float,
    ) -> int:
        return execute(
            sql=self.sql_update_daily_equity,
            api_file_name=self.api_file_name,
            params={
                "bot_id": bot_id,
                "snapshot_date": snapshot_date,
                "current_equity": current_equity,
            },
        )

    # -------------------------------------------------
    # POSITION VALUE
    # -------------------------------------------------

    def upsert_position_value(
        self,
        bot_id: str,
        uuid: str,
        pos_value: float,
        unrealized_pnl: float,
        updated_at: datetime,
    ) -> int:
        return execute(
            sql=self.sql_upsert_position_value,
            api_file_name=self.api_file_name,
            params={
                "bot_id": bot_id,
                "uuid": uuid,
                "pos_value": pos_value,
                "unrealized_pnl": unrealized_pnl,
                "updated_at": updated_at,
            },
        )

    def delete_position_value(self, bot_id: str, uuid: str) -> int:
        return execute(
            sql=self.sql_delete_position_value,
            api_file_name=self.api_file_name,
            params={
                "bot_id": bot_id,
                "uuid": uuid,
            },
        )
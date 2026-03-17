from __future__ import annotations

import time
from datetime import datetime

from Execution_layer.Executors.models import (
    CandidatePair,
    CloseDecision,
    OpenPairRecord,
)
from Execution_layer.Executors.symbol_mapper import ccxt_symbol_to_pybit_symbol


class ExecutorBase:
    """
    Core pair-trade workflow.

    Responsibilities:
    - load candidates
    - select one candidate
    - open trade
    - register open trade in shared state
    - monitor trade
    - close trade
    """

    def __init__(
        self,
        bot_config,
        worker_id: str,
        rules: dict[str, str],
        support_bridge,
        shared_state,
        repositories,
        order_manager,
        logger,
    ) -> None:
        self.bot_config = bot_config
        self.worker_id = worker_id
        self.rules = rules
        self.support_bridge = support_bridge
        self.shared_state = shared_state
        self.repositories = repositories
        self.order_manager = order_manager
        self.logger = logger

    # -------------------------------------------------------
    # MAIN ENTRY
    # -------------------------------------------------------

    def run_cycle(self) -> None:
        scheduler_status = self.repositories.get_scheduler_status(self.worker_id)

        if scheduler_status in {"SLEEP", "STOP", "SL_BLOCK"}:
            self.logger.info(
                "worker_id=%s skipping new open because scheduler_status=%s",
                self.worker_id,
                scheduler_status,
            )
            return

        ws_state = self.shared_state.get_ws_critical_state()
        if bool(ws_state.get("is_critical", False)):
            self.logger.warning(
                "worker_id=%s skipping new open because ws_critical comment=%s",
                self.worker_id,
                ws_state.get("comment"),
            )
            return

        candidates = self.load_candidates()

        if not candidates:
            self.logger.info("worker_id=%s no candidates found", self.worker_id)
            return

        candidate = self.select_candidate(candidates)

        if candidate is None:
            self.logger.info("worker_id=%s no candidate selected", self.worker_id)
            return

        record = self.try_open_candidate(candidate)

        if record is None:
            return

        self.monitor_open_trade(record)

    # -------------------------------------------------------
    # CANDIDATES
    # -------------------------------------------------------

    def load_candidates(self) -> list[CandidatePair]:
        return self.repositories.fetch_candidate_pool()

    def select_candidate(self, candidates: list[CandidatePair]) -> CandidatePair | None:
        """
        First version:
        - take first candidate that matches this bot level
        - skip quarantined
        - skip stale signal / stale pair_state
        """
        target_level = self.rules.get("level_180", "").strip()
        now = datetime.now()

        for candidate in candidates:
            if target_level and candidate.level_180 != target_level:
                continue

            if candidate.quarantine_reason:
                continue

            signal_age_sec = (now - candidate.signal_last_update_ts).total_seconds()
            if signal_age_sec > self.bot_config.signal_stale_sec:
                continue

            pair_state_age_sec = (now - candidate.pair_state_last_update_ts).total_seconds()
            if pair_state_age_sec > self.bot_config.pair_state_stale_sec:
                continue

            return candidate

        return None

    # -------------------------------------------------------
    # CONDITION SNAPSHOTS
    # -------------------------------------------------------

    def _fmt(self, value) -> str:
        return "None" if value is None else str(value)

    def build_trade_cond(self, candidate: CandidatePair) -> str:
        """
        Must include all pair_state values from adf till last_spread.
        Also includes z-score / TP / SL for trade context.
        """
        parts = [
            f"ADF:{self._fmt(candidate.adf)}",
            f"p-value:{self._fmt(candidate.p_value)}",
            f"Hurst:{self._fmt(candidate.hurst)}",
            f"HL:{self._fmt(candidate.hl)}",
            f"spread_skew:{self._fmt(candidate.spread_skew)}",
            f"spread_kurt:{self._fmt(candidate.spread_kurt)}",
            f"beta:{self._fmt(candidate.beta)}",
            f"beta_norm:{self._fmt(candidate.beta_norm)}",
            f"hl_spread_med:{self._fmt(candidate.hl_spread_med)}",
            f"last_spread:{self._fmt(candidate.last_spread)}",
            f"z-score:{self._fmt(candidate.last_z_score)}",
            f"TP:{self._fmt(candidate.tp)}",
            f"SL:{self._fmt(candidate.sl)}",
        ]
        return ",".join(parts)

    # -------------------------------------------------------
    # OPEN TRADE
    # -------------------------------------------------------

    def try_open_candidate(self, candidate: CandidatePair) -> OpenPairRecord | None:
        z = candidate.last_z_score

        if z is None:
            self.logger.warning("uuid=%s has no z-score", candidate.uuid)
            return None

        side_1, side_2 = self.get_entry_sides(z)

        # temporary in-memory duplicate guard
        if self.shared_state.get_open_pair(candidate.uuid) is not None:
            self.logger.warning(
                "uuid=%s already exists in shared_state, skipping open",
                candidate.uuid,
            )
            return None

        # real DB asset lock
        locked = self.repositories.try_lock_pair_assets(
            bot_id=self.bot_config.bot_id,
            uuid=candidate.uuid,
            asset_1=candidate.asset_1,
            asset_2=candidate.asset_2,
        )

        if not locked:
            self.logger.info(
                "uuid=%s assets already locked, skipping open",
                candidate.uuid,
            )
            return None

        last_price_1 = 1.0
        last_price_2 = 1.0

        try:
            ticker_1 = self.order_manager.client.fetch_ticker(candidate.asset_1)
            ticker_2 = self.order_manager.client.fetch_ticker(candidate.asset_2)

            last_price_1 = float(ticker_1.get("last") or 1.0)
            last_price_2 = float(ticker_2.get("last") or 1.0)

        except Exception as e:
            self.logger.error("price fetch failed for open uuid=%s err=%s", candidate.uuid, e)

            self.repositories.delete_asset_locks(
                bot_id=self.bot_config.bot_id,
                uuid=candidate.uuid,
            )
            return None

        total_exposure = float(self.rules.get("test_total_exposure", 1000))
        leverage = float(self.rules.get("test_leverage", 2))

        exposure_asset1 = total_exposure / 2.0
        exposure_asset2 = total_exposure / 2.0

        amount_asset1 = exposure_asset1 / last_price_1
        amount_asset2 = exposure_asset2 / last_price_2

        from Execution_layer.Executors.models import SizingResult

        sizing = SizingResult(
            leverage=leverage,
            exposure_asset1=exposure_asset1,
            exposure_asset2=exposure_asset2,
            amount_asset1=amount_asset1,
            amount_asset2=amount_asset2,
            price_asset1=last_price_1,
            price_asset2=last_price_2,
            total_exposure=total_exposure,
            beta_norm_used=float(candidate.beta_norm or 1.0),
            controller_leg=1,
        )

        result = self.order_manager.open_pair(
            candidate=candidate,
            side_1=side_1,
            side_2=side_2,
            sizing=sizing,
        )

        if not result.success:
            self.logger.error(
                "open failed uuid=%s worker_id=%s message=%s",
                candidate.uuid,
                self.worker_id,
                result.message,
            )

            self.repositories.delete_asset_locks(
                bot_id=self.bot_config.bot_id,
                uuid=candidate.uuid,
            )
            return None

        open_cond = self.build_trade_cond(candidate)

        trade_res_id = 0
        try:
            trade_res_id = int(
                self.repositories.insert_trade_open(
                    uuid=candidate.uuid,
                    bot_id=self.bot_config.bot_id,
                    pos_val=sizing.total_exposure,
                    open_cond=open_cond,
                )
            )
            self.logger.info(
                "trade_res open inserted uuid=%s worker_id=%s trade_res_id=%s",
                candidate.uuid,
                self.worker_id,
                trade_res_id,
            )
        except Exception as e:
            self.logger.exception(
                "trade_res open insert failed uuid=%s worker_id=%s err=%s",
                candidate.uuid,
                self.worker_id,
                e,
            )
            trade_res_id = 0

        if trade_res_id <= 0:
            self.logger.error(
                "trade_res open row missing uuid=%s worker_id=%s; trade will stay managed but DB lifecycle row was not created",
                candidate.uuid,
                self.worker_id,
            )

        record = OpenPairRecord(
            uuid=candidate.uuid,
            bot_id=self.bot_config.bot_id,
            trade_res_id=trade_res_id,
            asset_1=candidate.asset_1,
            asset_2=candidate.asset_2,
            ccxt_symbol_1=candidate.asset_1,
            ccxt_symbol_2=candidate.asset_2,
            pybit_symbol_1=ccxt_symbol_to_pybit_symbol(candidate.asset_1),
            pybit_symbol_2=ccxt_symbol_to_pybit_symbol(candidate.asset_2),
            side_1=side_1,
            side_2=side_2,
            open_ts=datetime.now(),
            initial_exposure=sizing.total_exposure,
            leverage=sizing.leverage,
            entry_z_score=float(z),
        )

        self.shared_state.register_open_pair(record)

        self.logger.info(
            "opened uuid=%s worker_id=%s side1=%s side2=%s exposure=%.2f trade_res_id=%s",
            candidate.uuid,
            self.worker_id,
            side_1,
            side_2,
            sizing.total_exposure,
            trade_res_id,
        )

        return record

    # -------------------------------------------------------
    # MONITOR
    # -------------------------------------------------------

    def monitor_open_trade(self, record: OpenPairRecord) -> None:
        self.logger.info("start monitoring uuid=%s worker_id=%s", record.uuid, self.worker_id)

        while True:
            candidate_refresh = self.repositories.fetch_candidate_by_uuid(record.uuid)
            pair_metrics = self.shared_state.get_pair_metrics(record.uuid)

            pair_unrealized_pnl = None
            if pair_metrics is not None:
                pair_unrealized_pnl = pair_metrics.unrealized_pnl

            env_close_decision = self.should_close_by_environment(
                record=record,
                candidate_refresh=candidate_refresh,
            )
            if env_close_decision.should_close:
                self.close_trade(record, env_close_decision.reason)
                return

            close_decision = self.should_close_by_trade_logic(
                record=record,
                candidate_refresh=candidate_refresh,
                pair_unrealized_pnl=pair_unrealized_pnl,
            )

            if close_decision.should_close:
                self.close_trade(record, close_decision.reason)
                return

            time.sleep(self.bot_config.worker_loop_sec)

    # -------------------------------------------------------
    # CLOSE TRADE
    # -------------------------------------------------------

    def close_trade(self, record: OpenPairRecord, close_reason: str) -> None:
        self.logger.warning(
            "closing uuid=%s worker_id=%s reason=%s trade_res_id=%s",
            record.uuid,
            self.worker_id,
            close_reason,
            record.trade_res_id,
        )

        result = self.order_manager.close_pair(record)

        if not result.success:
            self.logger.error(
                "close failed uuid=%s worker_id=%s message=%s | keeping locks/state because position may still be live",
                record.uuid,
                self.worker_id,
                result.message,
            )
            return

        candidate_refresh = self.repositories.fetch_candidate_by_uuid(record.uuid)
        close_cond = self.build_trade_cond(candidate_refresh) if candidate_refresh is not None else None

        pnl = self.order_manager.get_total_closed_pnl_for_trade(
            trade_id=record.trade_res_id,
            asset1_symbol=record.ccxt_symbol_1,
            asset2_symbol=record.ccxt_symbol_2,
            open_dt=record.open_ts,
        )

        pnl_pers = 0.0
        if record.initial_exposure > 0:
            pnl_pers = float(pnl) / float(record.initial_exposure)

        if record.trade_res_id > 0:
            try:
                updated_rows = self.repositories.update_trade_close(
                    trade_id=record.trade_res_id,
                    pnl=float(pnl),
                    pnl_pers=float(pnl_pers),
                    closed_by=close_reason,
                    close_cond=close_cond,
                )

                self.logger.info(
                    "trade_res close updated uuid=%s trade_res_id=%s updated_rows=%s pnl=%.8f pnl_pers=%.8f",
                    record.uuid,
                    record.trade_res_id,
                    updated_rows,
                    pnl,
                    pnl_pers,
                )
            except Exception as e:
                self.logger.exception(
                    "trade_res close update failed uuid=%s trade_res_id=%s err=%s",
                    record.uuid,
                    record.trade_res_id,
                    e,
                )
        else:
            self.logger.error(
                "trade_res close skipped uuid=%s worker_id=%s because trade_res_id is missing/invalid",
                record.uuid,
                self.worker_id,
            )

        deleted_locks = self.repositories.delete_asset_locks(
            bot_id=self.bot_config.bot_id,
            uuid=record.uuid,
        )

        self.logger.warning(
            "unlock result uuid=%s bot_id=%s deleted_rows=%s",
            record.uuid,
            self.bot_config.bot_id,
            deleted_locks,
        )

        deleted_pos_value = self.repositories.delete_position_value(
            bot_id=self.bot_config.bot_id,
            uuid=record.uuid,
        )

        self.logger.warning(
            "position_value cleanup uuid=%s bot_id=%s deleted_rows=%s",
            record.uuid,
            self.bot_config.bot_id,
            deleted_pos_value,
        )

        self.shared_state.remove_open_pair(record.uuid)
        self.shared_state.remove_pair_metrics(record.uuid)

    # -------------------------------------------------------
    # CONDITIONS
    # -------------------------------------------------------

    def get_entry_sides(self, z_score: float) -> tuple[str, str]:
        if z_score > 0:
            return "sell", "buy"
        return "buy", "sell"

    def should_close_by_environment(
        self,
        record: OpenPairRecord,
        candidate_refresh: CandidatePair | None,
    ) -> CloseDecision:
        now = datetime.now()

        scheduler_status = self.repositories.get_scheduler_status(self.worker_id)
        if scheduler_status in {"STOP", "SL_BLOCK"}:
            return CloseDecision(True, f"scheduler_{scheduler_status.lower()}")

        if candidate_refresh is None:
            return CloseDecision(True, "candidate_missing")

        signal_age_sec = (now - candidate_refresh.signal_last_update_ts).total_seconds()
        if signal_age_sec > self.bot_config.signal_stale_sec:
            return CloseDecision(True, "signal_stale")

        pair_state_age_sec = (now - candidate_refresh.pair_state_last_update_ts).total_seconds()
        if pair_state_age_sec > self.bot_config.pair_state_stale_sec:
            return CloseDecision(True, "pair_state_stale")

        ws_state = self.shared_state.get_ws_critical_state()
        if bool(ws_state.get("is_critical", False)):
            self.logger.warning(
                "uuid=%s worker_id=%s ws_critical detected comment=%s since=%s",
                record.uuid,
                self.worker_id,
                ws_state.get("comment"),
                ws_state.get("since"),
            )
            return CloseDecision(True, "ws_critical")

        return CloseDecision(False, "")

    def should_close_by_trade_logic(
        self,
        record: OpenPairRecord,
        candidate_refresh: CandidatePair | None,
        pair_unrealized_pnl: float | None,
    ) -> CloseDecision:
        if candidate_refresh is None:
            return CloseDecision(True, "candidate_missing")

        z = candidate_refresh.last_z_score

        if z is None:
            return CloseDecision(True, "z_score_missing")

        if (record.entry_z_score > 0 and z <= 0) or (record.entry_z_score < 0 and z >= 0):
            return CloseDecision(True, "z_score_cross_zero")

        if abs(z) >= float(self.rules.get("z_exit", 6)):
            return CloseDecision(True, "z_score_extreme")

        if pair_unrealized_pnl is not None and record.initial_exposure > 0:
            pnl_pct = (pair_unrealized_pnl / record.initial_exposure) * 100.0

            if pnl_pct >= float(candidate_refresh.tp):
                return CloseDecision(True, "take_profit")

            if pnl_pct <= -float(candidate_refresh.sl):
                return CloseDecision(True, "stop_loss")

        return CloseDecision(False, "")
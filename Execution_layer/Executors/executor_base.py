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
        - detailed rules will be added later
        """
        target_level = self.rules.get("level_180", "").strip()

        for candidate in candidates:
            if target_level and candidate.level_180 != target_level:
                continue

            if candidate.quarantine_reason:
                continue

            return candidate

        return None

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

        # first version sizing: placeholder simple structure
        # real sizing logic will be improved later
        last_price_1 = 1.0
        last_price_2 = 1.0

        try:
            ticker_1 = self.order_manager.client.fetch_ticker(candidate.asset_1)
            ticker_2 = self.order_manager.client.fetch_ticker(candidate.asset_2)

            last_price_1 = float(ticker_1.get("last") or 1.0)
            last_price_2 = float(ticker_2.get("last") or 1.0)

        except Exception as e:
            self.logger.error("price fetch failed for open uuid=%s err=%s", candidate.uuid, e)
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

        # NOTE:
        # first version uses dummy trade_res_id placeholder
        # later this will come from trade_res insert result
        trade_res_id = 0

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
            "opened uuid=%s worker_id=%s side1=%s side2=%s exposure=%.2f",
            candidate.uuid,
            self.worker_id,
            side_1,
            side_2,
            sizing.total_exposure,
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
            "closing uuid=%s worker_id=%s reason=%s",
            record.uuid,
            self.worker_id,
            close_reason,
        )

        result = self.order_manager.close_pair(record)

        if not result.success:
            self.logger.error(
                "close failed uuid=%s worker_id=%s message=%s",
                record.uuid,
                self.worker_id,
                result.message,
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

    def should_close_by_trade_logic(
        self,
        record: OpenPairRecord,
        candidate_refresh: CandidatePair | None,
        pair_unrealized_pnl: float | None,
    ) -> CloseDecision:
        """
        First version:
        - close on missing candidate refresh
        - close on z-score cross zero
        - close on extreme z-score
        - close on TP / SL by pair unrealized pnl
        """
        if candidate_refresh is None:
            return CloseDecision(True, "candidate_missing")

        z = candidate_refresh.last_z_score

        if z is None:
            return CloseDecision(True, "z_score_missing")

        # zero-cross logic
        if (record.entry_z_score > 0 and z <= 0) or (record.entry_z_score < 0 and z >= 0):
            return CloseDecision(True, "z_score_cross_zero")

        # extreme z
        if abs(z) >= float(self.rules.get("z_exit", 6)):
            return CloseDecision(True, "z_score_extreme")

        if pair_unrealized_pnl is not None and record.initial_exposure > 0:
            pnl_pct = (pair_unrealized_pnl / record.initial_exposure) * 100.0

            if pnl_pct >= float(candidate_refresh.tp):
                return CloseDecision(True, "take_profit")

            if pnl_pct <= -float(candidate_refresh.sl):
                return CloseDecision(True, "stop_loss")

        return CloseDecision(False, "")
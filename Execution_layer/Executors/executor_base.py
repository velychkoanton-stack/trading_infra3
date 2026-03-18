from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta

from Execution_layer.Executors.models import (
    CandidatePair,
    CloseDecision,
    OpenPairRecord,
    SizingResult,
)
from Execution_layer.Executors.symbol_mapper import ccxt_symbol_to_pybit_symbol

from Common.utils.telegram_sender import send_tg_message

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

    def is_account_ready_for_open(self) -> bool:
        account = self.support_bridge.get_account_snapshot()

        wallet_balance = float(account.get("wallet_balance") or 0.0)
        available_balance = float(account.get("available_balance") or 0.0)

        return wallet_balance > 0 and available_balance > 0
    
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

        if not self.is_account_ready_for_open():
            self.logger.info(
                "worker_id=%s skipping new open because account snapshot is not ready yet",
                self.worker_id,
            )
            return

        open_pairs = self.shared_state.get_open_pairs_for_bot(self.bot_config.bot_id)
        if len(open_pairs) >= self.bot_config.executor_threads_count:
            self.logger.info(
                "worker_id=%s skipping new open because open_pairs=%s >= executor_threads_count=%s",
                self.worker_id,
                len(open_pairs),
                self.bot_config.executor_threads_count,
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
        target_level = self.rules.get("level_180", "").strip()
        z_upper_threshold = float(self.rules.get("z_upper_threshold", 5))
        num_trades_180_min = int(float(self.rules.get("num_trades_180_min", 0)))
        num_trades_180_max = int(float(self.rules.get("num_trades_180_max", 999999)))

        return self.repositories.fetch_candidate_pool(
            level_180=target_level,
            z_upper_threshold=z_upper_threshold,
            num_trades_180_min=num_trades_180_min,
            num_trades_180_max=num_trades_180_max,
        )

    def select_candidate(self, candidates: list[CandidatePair]) -> CandidatePair | None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        for candidate in candidates:
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
    
    def _rule_float(self, key: str, default: float) -> float:
        try:
            return float(self.rules.get(key, default))
        except Exception:
            return float(default)

    def _rule_bool(self, key: str, default: bool) -> bool:
        raw = str(self.rules.get(key, str(default))).strip().lower()
        return raw in {"1", "true", "yes", "y", "on"}

    def calculate_position_sizing(self, candidate: CandidatePair) -> SizingResult | None:
        account = self.support_bridge.get_account_snapshot()

        wallet_balance = float(account.get("wallet_balance") or 0.0)
        available_balance = float(account.get("available_balance") or 0.0)

        if wallet_balance <= 0 or available_balance <= 0:
            self.logger.warning(
                "sizing skipped uuid=%s reason=balance_not_ready wallet_balance=%s available_balance=%s",
                candidate.uuid,
                wallet_balance,
                available_balance,
            )
            return None

        balance_cap = self._rule_float("balance_cap", wallet_balance)
        effective_balance = min(wallet_balance, balance_cap)

        balance_req = self._rule_float("balance_req", 0.20)
        exp_per_asset = self._rule_float("exp_per_asset", 0.30)
        bal_to_use = self._rule_float("bal_to_use", 0.10)
        exp_per_asset1 = self._rule_float("exp_per_asset1", 0.20)
        bal_to_use1 = self._rule_float("bal_to_use1", 0.20)
        max_lev = self._rule_float("max_lev", 5.0)
        min_per_leg_usdt = self._rule_float("min_per_leg_usdt", 800.0)
        respect_liquidity = self._rule_bool("respect_liquidity", True)
        beta_norm_min = self._rule_float("beta_norm_min", 0.8)
        beta_norm_max = self._rule_float("beta_norm_max", 1.2)

        if available_balance < (effective_balance * balance_req):
            self.logger.warning(
                "sizing rejected uuid=%s reason=available_balance_below_required available_balance=%.2f required=%.2f",
                candidate.uuid,
                available_balance,
                effective_balance * balance_req,
            )
            return None

        if available_balance > (effective_balance * balance_req):
            exposure_per_asset_base = exp_per_asset * effective_balance
            balance_to_use = bal_to_use * available_balance
        else:
            exposure_per_asset_base = exp_per_asset1 * effective_balance
            balance_to_use = bal_to_use1 * available_balance

        if balance_to_use <= 0:
            self.logger.warning("sizing rejected uuid=%s reason=balance_to_use_zero", candidate.uuid)
            return None

        try:
            beta_adj = float(candidate.beta_norm or 1.0)
        except Exception:
            beta_adj = 1.0

        beta_adj = max(beta_norm_min, min(beta_norm_max, beta_adj))

        ticker_1 = self.order_manager.client.fetch_ticker(candidate.asset_1)
        ticker_2 = self.order_manager.client.fetch_ticker(candidate.asset_2)

        price_1 = float(ticker_1.get("last") or 0.0)
        price_2 = float(ticker_2.get("last") or 0.0)

        if price_1 <= 0 or price_2 <= 0:
            self.logger.error("sizing failed uuid=%s reason=invalid_price", candidate.uuid)
            return None

        if respect_liquidity:
            v1_raw = float(candidate.asset1_5m_vol or 0.0)
            v2_raw = float(candidate.asset2_5m_vol or 0.0)

            v1_cap = max(min_per_leg_usdt, v1_raw)
            v2_cap = max(min_per_leg_usdt, v2_raw)

            ctrl_is_1 = (v1_raw <= v2_raw)

            e_ctrl = min(exposure_per_asset_base, v1_cap if ctrl_is_1 else v2_cap)
            e_other_prop = e_ctrl * beta_adj
            other_cap_eff = v2_cap if ctrl_is_1 else v1_cap

            if e_other_prop > other_cap_eff:
                scale = (other_cap_eff / e_other_prop) if e_other_prop > 0 else 0.0
                e_ctrl *= scale
                e_other_prop = other_cap_eff

            exposure_1 = e_ctrl if ctrl_is_1 else e_other_prop
            exposure_2 = e_other_prop if ctrl_is_1 else e_ctrl

            exposure_1 = max(exposure_1, min_per_leg_usdt)
            exposure_2 = max(exposure_2, min_per_leg_usdt)

            controller_leg = 1 if ctrl_is_1 else 2
        else:
            exposure_1 = max(exposure_per_asset_base, min_per_leg_usdt)
            exposure_2 = max(exposure_per_asset_base * beta_adj, min_per_leg_usdt)
            controller_leg = 1

        total_exposure = exposure_1 + exposure_2
        leverage = min(total_exposure / balance_to_use, max_lev)

        amount_1 = exposure_1 / price_1
        amount_2 = exposure_2 / price_2

        if amount_1 <= 0 or amount_2 <= 0:
            self.logger.error("sizing failed uuid=%s reason=non_positive_amount", candidate.uuid)
            return None

        self.logger.info(
            "sizing uuid=%s effective_balance=%.2f available_balance=%.2f exposure1=%.2f exposure2=%.2f total=%.2f lev=%.2f beta=%.4f liquidity=%s",
            candidate.uuid,
            effective_balance,
            available_balance,
            exposure_1,
            exposure_2,
            total_exposure,
            leverage,
            beta_adj,
            respect_liquidity,
        )

        return SizingResult(
            leverage=leverage,
            exposure_asset1=exposure_1,
            exposure_asset2=exposure_2,
            amount_asset1=amount_1,
            amount_asset2=amount_2,
            price_asset1=price_1,
            price_asset2=price_2,
            total_exposure=total_exposure,
            beta_norm_used=beta_adj,
            controller_leg=controller_leg,
        )

    def get_close_mode(self, close_reason: str) -> str:
        if close_reason in {"stop_loss", "scheduler_stop", "scheduler_sl_block"}:
            return "extreme"
        return "normal"
    
    def is_cointegration_lost(self, candidate: CandidatePair | None) -> bool:
        if candidate is None:
            return True

        if candidate.adf is None or candidate.p_value is None:
            return True

        return not (candidate.adf < -2.9 and candidate.p_value < 0.05)

    def clamp_hl_bars(self, hl_value: float | None) -> int:
        """
        Freeze HL in bars at open:
        - min 50
        - max 500
        """
        if hl_value is None:
            return 50

        try:
            hl_int = int(float(hl_value))
        except Exception:
            return 50

        if hl_int < 50:
            return 50
        if hl_int > 500:
            return 500
        return hl_int

    def send_telegram_message(self, text: str) -> None:
        try:
            ok = send_tg_message(
                text=text,
                api_file_name=self.bot_config.telegram_api_file,
            )
            if not ok:
                self.logger.warning("Telegram send failed | worker_id=%s", self.worker_id)
        except Exception as e:
            self.logger.exception("Telegram send exception worker_id=%s err=%s", self.worker_id, e)

    def build_open_tg_message(
        self,
        candidate: CandidatePair,
        record: OpenPairRecord,
    ) -> str:
        return (
            f"OPEN\n"
            f"bot={self.bot_config.bot_id}\n"
            f"worker={self.worker_id}\n"
            f"uuid={candidate.uuid}\n"
            f"pair={candidate.asset_1} | {candidate.asset_2}\n"
            f"side_1={record.side_1}\n"
            f"side_2={record.side_2}\n"
            f"entry_z={record.entry_z_score}\n"
            f"tp={candidate.tp}\n"
            f"sl={candidate.sl}\n"
            f"pos_val={record.initial_exposure}\n"
            f"trade_res_id={record.trade_res_id}"
        )

    def build_close_tg_message(
        self,
        record: OpenPairRecord,
        close_reason: str,
        pnl: float,
        pnl_pers: float,
    ) -> str:
        return (
            f"CLOSE\n"
            f"bot={self.bot_config.bot_id}\n"
            f"worker={self.worker_id}\n"
            f"uuid={record.uuid}\n"
            f"pair={record.asset_1} | {record.asset_2}\n"
            f"reason={close_reason}\n"
            f"pnl={pnl}\n"
            f"pnl_pers={pnl_pers}\n"
            f"pos_val={record.initial_exposure}\n"
            f"trade_res_id={record.trade_res_id}"
        )

    def send_critical_alert(self, title: str, details: str) -> None:
        text = (
            f"CRITICAL\n"
            f"bot={self.bot_config.bot_id}\n"
            f"worker={self.worker_id}\n"
            f"title={title}\n"
            f"details={details}"
        )
        self.send_telegram_message(text)

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

        sizing = self.calculate_position_sizing(candidate)

        if sizing is None:
            self.logger.warning(
                "uuid=%s sizing returned None, releasing locks",
                candidate.uuid,
            )
            self.repositories.delete_asset_locks(
                bot_id=self.bot_config.bot_id,
                uuid=candidate.uuid,
            )
            return None

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

            self.send_critical_alert(
                title="open_failed",
                details=f"uuid={candidate.uuid} message={result.message}",
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
            self.send_critical_alert(
                title="trade_res_open_insert_failed",
                details=f"uuid={candidate.uuid} err={e}",
            )
            trade_res_id = 0

        if trade_res_id <= 0:
            self.logger.error(
                "trade_res open row missing uuid=%s worker_id=%s; trade will stay managed but DB lifecycle row was not created",
                candidate.uuid,
                self.worker_id,
            )

        hl_bars_at_open = self.clamp_hl_bars(candidate.hl)
        hl_timeout_dt = datetime.now() + timedelta(minutes=hl_bars_at_open * 5)

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
            hl_bars_at_open=hl_bars_at_open,
            hl_timeout_dt=hl_timeout_dt,
        )

        self.shared_state.register_open_pair(record)

        self.logger.info(
            "opened uuid=%s worker_id=%s side1=%s side2=%s exposure=%.2f trade_res_id=%s hl_bars=%s hl_timeout_dt=%s",
            candidate.uuid,
            self.worker_id,
            side_1,
            side_2,
            sizing.total_exposure,
            trade_res_id,
            hl_bars_at_open,
            hl_timeout_dt,
        )

        self.send_telegram_message(
            self.build_open_tg_message(candidate, record)
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
        close_mode = self.get_close_mode(close_reason)

        self.logger.warning(
            "closing uuid=%s worker_id=%s reason=%s mode=%s trade_res_id=%s",
            record.uuid,
            self.worker_id,
            close_reason,
            close_mode,
            record.trade_res_id,
        )

        result = self.order_manager.close_pair(record, mode=close_mode)

        close_mode = self.get_close_mode(close_reason)
        result = self.order_manager.close_pair(record, mode=close_mode)

        if not result.success:
            self.logger.error(
                "close failed uuid=%s worker_id=%s message=%s | keeping locks/state because position may still be live",
                record.uuid,
                self.worker_id,
                result.message,
            )

            self.send_critical_alert(
                title="close_failed",
                details=f"uuid={record.uuid} message={result.message}",
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

                self.send_critical_alert(
                    title="trade_res_close_update_failed",
                    details=f"uuid={record.uuid} trade_res_id={record.trade_res_id} err={e}",
                )

        else:
            self.logger.error(
                "trade_res close skipped uuid=%s worker_id=%s because trade_res_id is missing/invalid",
                record.uuid,
                self.worker_id,
            )

        self.send_telegram_message(
            self.build_close_tg_message(
                record=record,
                close_reason=close_reason,
                pnl=float(pnl),
                pnl_pers=float(pnl_pers),
            )
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
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        scheduler_status = self.repositories.get_scheduler_status(self.worker_id)
        if scheduler_status in {"STOP", "SL_BLOCK"}:
            return CloseDecision(True, f"scheduler_{scheduler_status.lower()}")

        if candidate_refresh is None:
            return CloseDecision(True, "candidate_missing")
        
        pos1 = self.support_bridge.get_position(record.pybit_symbol_1)
        pos2 = self.support_bridge.get_position(record.pybit_symbol_2)

        leg1_open = pos1 is not None
        leg2_open = pos2 is not None

        if leg1_open != leg2_open:
            return CloseDecision(True, "leg_missing")


        signal_age_sec = (now - candidate_refresh.signal_last_update_ts).total_seconds()
        if signal_age_sec > self.bot_config.signal_stale_sec:
            return CloseDecision(True, "signal_stale")

        pair_state_age_sec = (now - candidate_refresh.pair_state_last_update_ts).total_seconds()
        if pair_state_age_sec > self.bot_config.pair_state_stale_sec:
            return CloseDecision(True, "pair_state_stale")

        ws_state = self.shared_state.get_ws_critical_state()
        if bool(ws_state.get("is_critical", False)):
            ws_comment = str(ws_state.get("comment") or "")
            ws_since = ws_state.get("since")

            self.logger.warning(
                "uuid=%s worker_id=%s ws_critical detected comment=%s since=%s",
                record.uuid,
                self.worker_id,
                ws_comment,
                ws_since,
            )

            # Do NOT close on private-only stale.
            # Private stream can be quiet even when connection is healthy.
            # Close only when public pricing side is stale.
            if "public=True" in ws_comment:
                return CloseDecision(True, "ws_critical_public")

            # private-only stale -> warning only, continue monitoring

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

        # ---------------------------------------------------
        # HL timeout cointegration check
        # Before HL timeout: do NOT check cointegration-loss exit
        # After HL timeout: if cointegration lost -> close
        # ---------------------------------------------------
        now = datetime.now()
        if now >= record.hl_timeout_dt:
            if self.is_cointegration_lost(candidate_refresh):
                return CloseDecision(True, "HL timeout & lost cointegration")

        # ---------------------------------------------------
        # Extreme z-score + cointegration lost
        # ---------------------------------------------------
        z_exit = float(self.rules.get("z_exit", 6))
        if abs(z) > z_exit:
            if self.is_cointegration_lost(candidate_refresh):
                return CloseDecision(True, "Stop_loss_threshold & lost cointegration")

        # ---------------------------------------------------
        # Zero-cross logic
        # ---------------------------------------------------
        if (record.entry_z_score > 0 and z <= 0) or (record.entry_z_score < 0 and z >= 0):
            return CloseDecision(True, "z_score_cross_zero")

        # ---------------------------------------------------
        # TP / SL by pair unrealized pnl
        # tp/sl are stored as fraction: 0.02 = 2%
        # ---------------------------------------------------
        if pair_unrealized_pnl is not None and record.initial_exposure > 0:
            pnl_fraction = pair_unrealized_pnl / record.initial_exposure

            if pnl_fraction >= float(candidate_refresh.tp):
                return CloseDecision(True, "take_profit")

            if pnl_fraction <= -float(candidate_refresh.sl):
                return CloseDecision(True, "stop_loss")

        return CloseDecision(False, "")
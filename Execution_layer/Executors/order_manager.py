from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Optional

import ccxt

from Common.exchange.bybit_client import create_bybit_client
from Execution_layer.Executors.models import (
    CandidatePair,
    OpenPairRecord,
    OrderExecutionResult,
    SizingResult,
)


class OrderManager:

    def __init__(
        self,
        api_file_name: str,
        environment: str,
        logger,
        rules: dict[str, str],
    ) -> None:

        self.api_file_name = api_file_name
        self.environment = environment
        self.logger = logger
        self.rules = rules

        self.client: ccxt.bybit = create_bybit_client(
            api_file_name=self.api_file_name,
            demo=(self.environment == "demo"),
        )

    # -------------------------------------------------------
    # LEVERAGE
    # -------------------------------------------------------

    def set_leverage_with_retry(self, symbol: str, leverage: float) -> bool:
        leverage = float(leverage)

        for attempt in range(3):
            try:
                self.client.set_leverage(leverage, symbol)
                return True
            except Exception as e:
                self.logger.warning(
                    "set_leverage attempt=%s failed symbol=%s err=%s",
                    attempt + 1,
                    symbol,
                    e,
                )
                time.sleep(1)

        return False

    # -------------------------------------------------------
    # MARKET ORDER
    # -------------------------------------------------------

    def place_market_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        leverage: float,
    ) -> Optional[dict]:

        try:
            self.set_leverage_with_retry(symbol, leverage)

            order = self.client.create_order(
                symbol=symbol,
                type="market",
                side=side,
                amount=amount,
            )
            return order

        except Exception as e:
            self.logger.error(
                "Market order failed symbol=%s side=%s amount=%s err=%s",
                symbol,
                side,
                amount,
                e,
            )
            return None

    # -------------------------------------------------------
    # POSITION HELPERS
    # -------------------------------------------------------

    def fetch_position_amount(self, symbol: str) -> float:
        try:
            positions = self.client.fetch_positions([symbol])

            if not positions:
                return 0.0

            pos = positions[0]
            size = float(pos.get("contracts", 0) or 0.0)
            return abs(size)

        except Exception as e:
            self.logger.error("fetch_position_amount error %s %s", symbol, e)
            return 0.0

    def _get_reverse_side(self, side: str) -> str:
        return "sell" if str(side).strip().lower() == "buy" else "buy"

    def _is_flat_pair(self, record: OpenPairRecord) -> tuple[bool, float, float]:
        remaining1 = self.fetch_position_amount(record.ccxt_symbol_1)
        remaining2 = self.fetch_position_amount(record.ccxt_symbol_2)
        is_flat = remaining1 <= 0 and remaining2 <= 0
        return is_flat, remaining1, remaining2

    # -------------------------------------------------------
    # EMERGENCY FLATTEN HELPERS
    # -------------------------------------------------------

    def flatten_symbol_position(
        self,
        symbol: str,
        original_side: str,
        leverage: float,
        fallback_amount: float,
    ) -> bool:
        """
        Emergency flatten for one symbol.
        """
        try:
            live_amount = self.fetch_position_amount(symbol)
            amount_to_close = float(live_amount or 0.0)

            if amount_to_close <= 0:
                amount_to_close = float(fallback_amount or 0.0)

            if amount_to_close <= 0:
                self.logger.warning(
                    "flatten skip symbol=%s because close amount is zero",
                    symbol,
                )
                return True

            reverse_side = self._get_reverse_side(original_side)

            self.logger.warning(
                "emergency flatten start symbol=%s reverse_side=%s amount=%.10f",
                symbol,
                reverse_side,
                amount_to_close,
            )

            order = self.place_market_order(
                symbol=symbol,
                side=reverse_side,
                amount=amount_to_close,
                leverage=leverage,
            )

            if not order:
                self.logger.error(
                    "emergency flatten order failed symbol=%s reverse_side=%s amount=%.10f",
                    symbol,
                    reverse_side,
                    amount_to_close,
                )
                return False

            time.sleep(2.0)

            remaining = self.fetch_position_amount(symbol)

            if remaining > 0:
                self.logger.error(
                    "emergency flatten verify failed symbol=%s remaining=%.10f",
                    symbol,
                    remaining,
                )
                return False

            self.logger.warning("emergency flatten success symbol=%s", symbol)
            return True

        except Exception as e:
            self.logger.exception("flatten_symbol_position failure symbol=%s err=%s", symbol, e)
            return False

    def _emergency_close_remaining(self, record: OpenPairRecord) -> OrderExecutionResult:
        """
        Emergency flatten any remaining live legs.
        """
        order_ids: list[str] = []

        try:
            amount1 = self.fetch_position_amount(record.ccxt_symbol_1)
            amount2 = self.fetch_position_amount(record.ccxt_symbol_2)

            ok1 = True
            ok2 = True

            if amount1 > 0:
                reverse1 = self._get_reverse_side(record.side_1)
                order1 = self.place_market_order(
                    symbol=record.ccxt_symbol_1,
                    side=reverse1,
                    amount=amount1,
                    leverage=record.leverage,
                )
                if order1:
                    order_ids.append(order1["id"])
                else:
                    ok1 = False

            if amount2 > 0:
                reverse2 = self._get_reverse_side(record.side_2)
                order2 = self.place_market_order(
                    symbol=record.ccxt_symbol_2,
                    side=reverse2,
                    amount=amount2,
                    leverage=record.leverage,
                )
                if order2:
                    order_ids.append(order2["id"])
                else:
                    ok2 = False

            time.sleep(2.0)

            is_flat, remaining1, remaining2 = self._is_flat_pair(record)

            if is_flat:
                return OrderExecutionResult(
                    True,
                    "pair_closed_emergency",
                    order_ids,
                )

            return OrderExecutionResult(
                False,
                f"emergency_close_verify_failed remaining1={remaining1} remaining2={remaining2} leg1_ok={ok1} leg2_ok={ok2}",
                order_ids,
            )

        except Exception as e:
            self.logger.exception("emergency close failure")
            return OrderExecutionResult(False, str(e), order_ids)

    # -------------------------------------------------------
    # OPEN PAIR
    # -------------------------------------------------------

    def open_pair(
        self,
        candidate: CandidatePair,
        side_1: str,
        side_2: str,
        sizing: SizingResult,
    ) -> OrderExecutionResult:

        symbol_1 = candidate.asset_1
        symbol_2 = candidate.asset_2

        order_ids: list[str] = []

        try:
            order1 = self.place_market_order(
                symbol_1,
                side_1,
                sizing.amount_asset1,
                sizing.leverage,
            )

            if not order1:
                return OrderExecutionResult(False, "leg1_failed", [])

            order_ids.append(order1["id"])

            self.logger.warning(
                "open_pair leg1 success uuid=%s symbol=%s side=%s amount=%.10f",
                candidate.uuid,
                symbol_1,
                side_1,
                sizing.amount_asset1,
            )

            order2 = self.place_market_order(
                symbol_2,
                side_2,
                sizing.amount_asset2,
                sizing.leverage,
            )

            if not order2:
                self.logger.error(
                    "open_pair leg2 failed uuid=%s symbol=%s side=%s amount=%.10f | starting emergency flatten for leg1",
                    candidate.uuid,
                    symbol_2,
                    side_2,
                    sizing.amount_asset2,
                )

                flatten_ok = self.flatten_symbol_position(
                    symbol=symbol_1,
                    original_side=side_1,
                    leverage=sizing.leverage,
                    fallback_amount=sizing.amount_asset1,
                )

                if flatten_ok:
                    return OrderExecutionResult(
                        False,
                        "leg2_failed_leg1_flattened",
                        order_ids,
                    )

                return OrderExecutionResult(
                    False,
                    "leg2_failed_leg1_flatten_failed",
                    order_ids,
                )

            order_ids.append(order2["id"])

            return OrderExecutionResult(
                success=True,
                message="pair_opened",
                order_ids=order_ids,
                filled_notional_1=sizing.exposure_asset1,
                filled_notional_2=sizing.exposure_asset2,
            )

        except Exception as e:
            self.logger.exception("open_pair failure")
            return OrderExecutionResult(False, str(e), order_ids)

    # -------------------------------------------------------
    # CLOSE PAIR
    # -------------------------------------------------------

    def _close_pair_once(self, record: OpenPairRecord) -> OrderExecutionResult:
        """
        One normal close attempt plus verification.
        """
        order_ids: list[str] = []

        try:
            amount1 = self.fetch_position_amount(record.ccxt_symbol_1)
            amount2 = self.fetch_position_amount(record.ccxt_symbol_2)

            if amount1 > 0:
                side = self._get_reverse_side(record.side_1)

                o = self.place_market_order(
                    record.ccxt_symbol_1,
                    side,
                    amount1,
                    record.leverage,
                )

                if not o:
                    return OrderExecutionResult(
                        False,
                        f"close_leg1_failed amount={amount1}",
                        order_ids,
                    )

                order_ids.append(o["id"])

            if amount2 > 0:
                side = self._get_reverse_side(record.side_2)

                o = self.place_market_order(
                    record.ccxt_symbol_2,
                    side,
                    amount2,
                    record.leverage,
                )

                if not o:
                    return OrderExecutionResult(
                        False,
                        f"close_leg2_failed amount={amount2}",
                        order_ids,
                    )

                order_ids.append(o["id"])

            time.sleep(2.0)

            is_flat, remaining1, remaining2 = self._is_flat_pair(record)

            if not is_flat:
                return OrderExecutionResult(
                    False,
                    f"close_verify_failed remaining1={remaining1} remaining2={remaining2}",
                    order_ids,
                )

            return OrderExecutionResult(
                True,
                "pair_closed",
                order_ids,
            )

        except Exception as e:
            self.logger.exception("close_pair_once failure")
            return OrderExecutionResult(False, str(e), order_ids)

    def close_pair(self, record: OpenPairRecord) -> OrderExecutionResult:
        """
        Close workflow:
        1. normal close attempts
        2. if still not flat -> emergency close
        """
        all_order_ids: list[str] = []

        normal_retries = int(float(self.rules.get("close_retries", 3)))
        retry_sleep_sec = float(self.rules.get("close_retry_sleep_sec", 2))

        for attempt in range(1, normal_retries + 1):
            self.logger.warning(
                "close_pair normal attempt=%s uuid=%s symbols=%s/%s",
                attempt,
                record.uuid,
                record.ccxt_symbol_1,
                record.ccxt_symbol_2,
            )

            result = self._close_pair_once(record)
            all_order_ids.extend(result.order_ids)

            if result.success:
                return OrderExecutionResult(
                    True,
                    f"pair_closed_normal_attempt_{attempt}",
                    all_order_ids,
                )

            self.logger.error(
                "close_pair normal attempt failed uuid=%s attempt=%s message=%s",
                record.uuid,
                attempt,
                result.message,
            )

            if attempt < normal_retries:
                time.sleep(retry_sleep_sec)

        self.logger.error(
            "close_pair switching to EMERGENCY mode uuid=%s",
            record.uuid,
        )

        emergency_result = self._emergency_close_remaining(record)
        all_order_ids.extend(emergency_result.order_ids)

        if emergency_result.success:
            return OrderExecutionResult(
                True,
                emergency_result.message,
                all_order_ids,
            )

        return OrderExecutionResult(
            False,
            emergency_result.message,
            all_order_ids,
        )

    # -------------------------------------------------------
    # REALIZED PNL
    # -------------------------------------------------------

    def _sum_closed_pnl_from_positions(self, positions: list) -> float:
        """
        Sum parsed PnL from CCXT positions history.
        Prefer parsed 'pnl'; fallback to raw 'info.closedPnl'.
        """
        total = 0.0

        for p in positions or []:
            pnl = p.get("pnl")

            if pnl is None:
                try:
                    pnl = float(p.get("info", {}).get("closedPnl") or 0.0)
                except Exception:
                    pnl = 0.0

            total += float(pnl or 0.0)

        return total

    def _fetch_positions_history_page(
        self,
        symbol: str,
        since_ms: int,
        until_ms: int,
        limit: int = 100,
    ) -> list:
        """
        Single page fetch via CCXT.
        """
        return self.client.fetch_positions_history(
            [symbol],   # CCXT expects single-symbol list
            since_ms,
            limit,
            {
                "until": until_ms,
                "subType": "linear",
            },
        )

    def sum_closed_pnl_for_window(
        self,
        symbol: str,
        since_ms: int,
        until_ms: int,
    ) -> float:
        """
        Sum closed PnL for a symbol over [since_ms, until_ms].
        For current executor lifecycle, one page is enough.
        """
        positions = self._fetch_positions_history_page(
            symbol=symbol,
            since_ms=since_ms,
            until_ms=until_ms,
            limit=100,
        )
        return self._sum_closed_pnl_from_positions(positions)

    def get_total_closed_pnl_for_trade(
        self,
        trade_id: int,
        asset1_symbol: str,
        asset2_symbol: str,
        open_dt: datetime,
    ) -> float:
        """
        Sum realized closed PnL across both legs for this trade window.

        Uses:
        - open_dt as window start
        - current time as window end

        Includes retries because broker history can lag slightly after close.
        """
        try:
            # open_dt is created with datetime.now() in local server time.
            # Use local timestamp conversion directly.
            since_ms = int(open_dt.timestamp() * 1000)

            retries = 4
            sleep_sec = 2.0
            last_total = 0.0

            for attempt in range(1, retries + 1):
                until_ms = int(time.time() * 1000)

                # Safety guard
                if since_ms >= until_ms:
                    self.logger.warning(
                        "realized pnl window invalid attempt=%s trade_id=%s since_ms=%s until_ms=%s | adjusting since_ms",
                        attempt,
                        trade_id,
                        since_ms,
                        until_ms,
                    )
                    since_ms = max(until_ms - 60_000, 0)

                pnl1 = self.sum_closed_pnl_for_window(
                    symbol=asset1_symbol,
                    since_ms=since_ms,
                    until_ms=until_ms,
                )
                pnl2 = self.sum_closed_pnl_for_window(
                    symbol=asset2_symbol,
                    since_ms=since_ms,
                    until_ms=until_ms,
                )

                total = float(pnl1 + pnl2)
                last_total = total

                self.logger.info(
                    "realized pnl fetch attempt=%s trade_id=%s symbols=%s/%s pnl1=%.8f pnl2=%.8f total=%.8f",
                    attempt,
                    trade_id,
                    asset1_symbol,
                    asset2_symbol,
                    pnl1,
                    pnl2,
                    total,
                )

                if total != 0.0:
                    return total

                if attempt < retries:
                    time.sleep(sleep_sec)

            return last_total

        except Exception as e:
            self.logger.error(f"pnl fetch error {e}")
            return 0.0
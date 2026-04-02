from __future__ import annotations

import time
from datetime import datetime
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
    # RULE HELPERS
    # -------------------------------------------------------

    def _rule_float(self, key: str, default: float) -> float:
        try:
            return float(self.rules.get(key, default))
        except Exception:
            return float(default)

    def _rule_int(self, key: str, default: int) -> int:
        try:
            return int(float(self.rules.get(key, default)))
        except Exception:
            return int(default)

    # -------------------------------------------------------
    # LEVERAGE
    # -------------------------------------------------------

    def set_leverage_with_retry(self, symbol: str, leverage: float) -> bool:
        leverage = float(leverage)
        max_lev = self._rule_float("max_lev", 5.0)
        leverage = min(max(leverage, 1.0), max_lev)

        for attempt in range(3):
            try:
                self.client.set_leverage(leverage, symbol)
                return True

            except Exception as e:
                msg = str(e)

                # Bybit normal harmless case
                if "110043" in msg or "leverage not modified" in msg:
                    self.logger.warning(
                        "set_leverage attempt=%s failed symbol=%s err=%s",
                        attempt + 1,
                        symbol,
                        e,
                    )
                    return True

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
    # POSITION / PRICE HELPERS
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

    def fetch_last_price(self, symbol: str) -> float:
        try:
            ticker = self.client.fetch_ticker(symbol)
            return float(ticker.get("last") or 0.0)
        except Exception as e:
            self.logger.error("fetch_last_price error %s %s", symbol, e)
            return 0.0

    def _get_reverse_side(self, side: str) -> str:
        return "sell" if str(side).strip().lower() == "buy" else "buy"

    def _is_flat_pair(self, record: OpenPairRecord) -> tuple[bool, float, float]:
        remaining1 = self.fetch_position_amount(record.ccxt_symbol_1)
        remaining2 = self.fetch_position_amount(record.ccxt_symbol_2)
        is_flat = remaining1 <= 0 and remaining2 <= 0
        return is_flat, remaining1, remaining2

    # -------------------------------------------------------
    # CHUNK HELPERS
    # -------------------------------------------------------

    def _amount_from_notional(self, symbol: str, notional_usdt: float) -> float:
        price = self.fetch_last_price(symbol)
        if price <= 0:
            return 0.0
        return float(notional_usdt) / price

    def _chunk_pair_open(
        self,
        symbol_1: str,
        side_1: str,
        target_usdt_1: float,
        symbol_2: str,
        side_2: str,
        target_usdt_2: float,
        leverage: float,
    ) -> OrderExecutionResult:
        chunk_usdt = self._rule_float("chunk_usdt", 100.0)
        pause_sec = self._rule_float("chunk_pause_sec", 3.0)

        order_ids: list[str] = []
        rem1 = float(max(0.0, target_usdt_1))
        rem2 = float(max(0.0, target_usdt_2))

        filled_amt_1 = 0.0
        filled_amt_2 = 0.0

        while rem1 > 1e-8 or rem2 > 1e-8:
            if rem1 > 1e-8:
                notional1 = min(chunk_usdt, rem1)
                amt1 = self._amount_from_notional(symbol_1, notional1)
                if amt1 <= 0:
                    return OrderExecutionResult(False, "leg1_price_invalid", order_ids)

                o1 = self.place_market_order(symbol_1, side_1, amt1, leverage)
                if not o1:
                    self.logger.error(
                        "chunk open failed leg1 symbol=%s side=%s amount=%.10f",
                        symbol_1,
                        side_1,
                        amt1,
                    )
                    if filled_amt_1 > 0:
                        self.flatten_symbol_position(
                            symbol=symbol_1,
                            original_side=side_1,
                            leverage=leverage,
                            fallback_amount=filled_amt_1,
                        )
                    if filled_amt_2 > 0:
                        self.flatten_symbol_position(
                            symbol=symbol_2,
                            original_side=side_2,
                            leverage=leverage,
                            fallback_amount=filled_amt_2,
                        )
                    return OrderExecutionResult(False, "leg1_chunk_failed", order_ids)

                order_ids.append(o1["id"])
                filled_amt_1 += amt1
                rem1 -= notional1

            if rem2 > 1e-8:
                notional2 = min(chunk_usdt, rem2)
                amt2 = self._amount_from_notional(symbol_2, notional2)
                if amt2 <= 0:
                    if filled_amt_1 > 0:
                        self.flatten_symbol_position(
                            symbol=symbol_1,
                            original_side=side_1,
                            leverage=leverage,
                            fallback_amount=filled_amt_1,
                        )
                    if filled_amt_2 > 0:
                        self.flatten_symbol_position(
                            symbol=symbol_2,
                            original_side=side_2,
                            leverage=leverage,
                            fallback_amount=filled_amt_2,
                        )
                    return OrderExecutionResult(False, "leg2_price_invalid", order_ids)

                o2 = self.place_market_order(symbol_2, side_2, amt2, leverage)
                if not o2:
                    self.logger.error(
                        "chunk open failed leg2 symbol=%s side=%s amount=%.10f",
                        symbol_2,
                        side_2,
                        amt2,
                    )
                    if filled_amt_1 > 0:
                        self.flatten_symbol_position(
                            symbol=symbol_1,
                            original_side=side_1,
                            leverage=leverage,
                            fallback_amount=filled_amt_1,
                        )
                    if filled_amt_2 > 0:
                        self.flatten_symbol_position(
                            symbol=symbol_2,
                            original_side=side_2,
                            leverage=leverage,
                            fallback_amount=filled_amt_2,
                        )
                    return OrderExecutionResult(False, "leg2_chunk_failed", order_ids)

                order_ids.append(o2["id"])
                filled_amt_2 += amt2
                rem2 -= notional2

            time.sleep(pause_sec)

        return OrderExecutionResult(True, "pair_opened", order_ids)

    def _close_pair_normal_chopped_once(self, record: OpenPairRecord) -> OrderExecutionResult:
        chunk_usdt = self._rule_float("chunk_usdt", 100.0)
        pause_sec = self._rule_float("chunk_pause_sec", 3.0)

        order_ids: list[str] = []

        while True:
            amount1 = self.fetch_position_amount(record.ccxt_symbol_1)
            amount2 = self.fetch_position_amount(record.ccxt_symbol_2)

            if amount1 <= 0 and amount2 <= 0:
                return OrderExecutionResult(True, "pair_closed_normal", order_ids)

            if amount1 > 0:
                price1 = self.fetch_last_price(record.ccxt_symbol_1)
                if price1 <= 0:
                    return OrderExecutionResult(False, "close_leg1_price_invalid", order_ids)

                close_notional1 = min(chunk_usdt, amount1 * price1)
                close_amt1 = close_notional1 / price1
                side1 = self._get_reverse_side(record.side_1)

                o1 = self.place_market_order(
                    record.ccxt_symbol_1,
                    side1,
                    close_amt1,
                    record.leverage,
                )
                if not o1:
                    return OrderExecutionResult(False, "close_leg1_chunk_failed", order_ids)

                order_ids.append(o1["id"])

            if amount2 > 0:
                price2 = self.fetch_last_price(record.ccxt_symbol_2)
                if price2 <= 0:
                    return OrderExecutionResult(False, "close_leg2_price_invalid", order_ids)

                close_notional2 = min(chunk_usdt, amount2 * price2)
                close_amt2 = close_notional2 / price2
                side2 = self._get_reverse_side(record.side_2)

                o2 = self.place_market_order(
                    record.ccxt_symbol_2,
                    side2,
                    close_amt2,
                    record.leverage,
                )
                if not o2:
                    return OrderExecutionResult(False, "close_leg2_chunk_failed", order_ids)

                order_ids.append(o2["id"])

            time.sleep(pause_sec)

    def _close_pair_extreme_market_once(self, record: OpenPairRecord) -> OrderExecutionResult:
        order_ids: list[str] = []

        amount1 = self.fetch_position_amount(record.ccxt_symbol_1)
        amount2 = self.fetch_position_amount(record.ccxt_symbol_2)

        if amount1 > 0:
            side1 = self._get_reverse_side(record.side_1)
            o1 = self.place_market_order(
                record.ccxt_symbol_1,
                side1,
                amount1,
                record.leverage,
            )
            if not o1:
                return OrderExecutionResult(False, f"extreme_close_leg1_failed amount={amount1}", order_ids)
            order_ids.append(o1["id"])

        if amount2 > 0:
            side2 = self._get_reverse_side(record.side_2)
            o2 = self.place_market_order(
                record.ccxt_symbol_2,
                side2,
                amount2,
                record.leverage,
            )
            if not o2:
                return OrderExecutionResult(False, f"extreme_close_leg2_failed amount={amount2}", order_ids)
            order_ids.append(o2["id"])

        time.sleep(2.0)

        is_flat, remaining1, remaining2 = self._is_flat_pair(record)
        if not is_flat:
            return OrderExecutionResult(
                False,
                f"extreme_close_verify_failed remaining1={remaining1} remaining2={remaining2}",
                order_ids,
            )

        return OrderExecutionResult(True, "pair_closed_extreme", order_ids)

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
        try:
            live_amount = self.fetch_position_amount(symbol)
            amount_to_close = float(live_amount or 0.0)

            if amount_to_close <= 0:
                amount_to_close = float(fallback_amount or 0.0)

            if amount_to_close <= 0:
                self.logger.warning("flatten skip symbol=%s because close amount is zero", symbol)
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
        result = self._chunk_pair_open(
            symbol_1=candidate.asset_1,
            side_1=side_1,
            target_usdt_1=sizing.exposure_asset1,
            symbol_2=candidate.asset_2,
            side_2=side_2,
            target_usdt_2=sizing.exposure_asset2,
            leverage=sizing.leverage,
        )

        if result.success:
            result.filled_notional_1 = sizing.exposure_asset1
            result.filled_notional_2 = sizing.exposure_asset2

        return result

    # -------------------------------------------------------
    # CLOSE PAIR
    # -------------------------------------------------------

    def close_pair(self, record: OpenPairRecord, mode: str = "normal") -> OrderExecutionResult:
        all_order_ids: list[str] = []

        retries = self._rule_int("close_retries", 3)
        retry_sleep_sec = self._rule_float("close_retry_sleep_sec", 2.0)

        for attempt in range(1, retries + 1):
            self.logger.warning(
                "close_pair mode=%s attempt=%s uuid=%s symbols=%s/%s",
                mode,
                attempt,
                record.uuid,
                record.ccxt_symbol_1,
                record.ccxt_symbol_2,
            )

            if mode == "extreme":
                result = self._close_pair_extreme_market_once(record)
            else:
                result = self._close_pair_normal_chopped_once(record)

            all_order_ids.extend(result.order_ids)

            if result.success:
                return OrderExecutionResult(True, result.message, all_order_ids)

            self.logger.error(
                "close_pair attempt failed uuid=%s attempt=%s mode=%s message=%s",
                record.uuid,
                attempt,
                mode,
                result.message,
            )

            if attempt < retries:
                time.sleep(retry_sleep_sec)

        # safety fallback: if normal mode failed, escalate to extreme
        if mode != "extreme":
            self.logger.error("close_pair escalating to EXTREME mode uuid=%s", record.uuid)
            extreme_result = self._close_pair_extreme_market_once(record)
            all_order_ids.extend(extreme_result.order_ids)

            if extreme_result.success:
                return OrderExecutionResult(True, extreme_result.message, all_order_ids)

            return OrderExecutionResult(False, extreme_result.message, all_order_ids)

        return OrderExecutionResult(False, f"close_failed_after_retries mode={mode}", all_order_ids)

    # -------------------------------------------------------
    # REALIZED PNL
    # -------------------------------------------------------

    def _sum_closed_pnl_from_positions(self, positions: list) -> float:
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
        return self.client.fetch_positions_history(
            [symbol],
            since_ms,
            limit,
            {"until": until_ms, "subType": "linear"},
        )

    def sum_closed_pnl_for_window(
        self,
        symbol: str,
        since_ms: int,
        until_ms: int,
    ) -> float:
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
        try:
            since_ms = int(open_dt.timestamp() * 1000)

            retries = 4
            sleep_sec = 2.0
            last_total = 0.0

            for attempt in range(1, retries + 1):
                until_ms = int(time.time() * 1000)

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
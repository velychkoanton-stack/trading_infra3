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
                    f"set_leverage attempt={attempt+1} failed symbol={symbol} err={e}"
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
                f"Market order failed symbol={symbol} side={side} amount={amount} err={e}"
            )

            return None

    # -------------------------------------------------------
    # FETCH POSITION AMOUNT
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

            self.logger.error(f"fetch_position_amount error {symbol} {e}")

            return 0.0

    # -------------------------------------------------------
    # EMERGENCY FLATTEN HELPERS
    # -------------------------------------------------------

    def _get_reverse_side(self, side: str) -> str:
        side_norm = str(side).strip().lower()
        if side_norm == "buy":
            return "sell"
        return "buy"

    def flatten_symbol_position(
        self,
        symbol: str,
        original_side: str,
        leverage: float,
        fallback_amount: float,
    ) -> bool:
        """
        Emergency flatten for one symbol.

        Tries to read current live position size from broker first.
        Falls back to the originally intended order amount if needed.
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

            self.logger.warning(
                "emergency flatten success symbol=%s",
                symbol,
            )
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

    def close_pair(self, record: OpenPairRecord) -> OrderExecutionResult:

        order_ids: list[str] = []

        try:
            amount1 = self.fetch_position_amount(record.ccxt_symbol_1)
            amount2 = self.fetch_position_amount(record.ccxt_symbol_2)

            if amount1 > 0:
                side = "sell" if record.side_1 == "buy" else "buy"

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
                side = "sell" if record.side_2 == "buy" else "buy"

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

            # brief settle time before verification
            time.sleep(2.0)

            remaining1 = self.fetch_position_amount(record.ccxt_symbol_1)
            remaining2 = self.fetch_position_amount(record.ccxt_symbol_2)

            if remaining1 > 0 or remaining2 > 0:
                return OrderExecutionResult(
                    False,
                    f"close_verify_failed remaining1={remaining1} remaining2={remaining2}",
                    order_ids,
                )

            return OrderExecutionResult(
                success=True,
                message="pair_closed",
                order_ids=order_ids,
            )

        except Exception as e:

            self.logger.exception("close_pair failure")

            return OrderExecutionResult(False, str(e), order_ids)

    # -------------------------------------------------------
    # REALIZED PNL
    # -------------------------------------------------------

    def get_total_closed_pnl_for_trade(
        self,
        trade_id: int,
        asset1_symbol: str,
        asset2_symbol: str,
        open_dt: datetime,
    ) -> float:

        """
        Simplified version for first stage.

        Later we will port your old executor logic here.
        """

        try:

            pnl_total = 0.0

            for symbol in [asset1_symbol, asset2_symbol]:

                history = self.client.fetch_my_trades(symbol)

                for trade in history:

                    ts = trade["timestamp"] / 1000

                    if ts >= open_dt.timestamp():

                        pnl_total += float(trade.get("info", {}).get("closedPnl", 0))

            return pnl_total

        except Exception as e:

            self.logger.error(f"pnl fetch error {e}")

            return 0.0
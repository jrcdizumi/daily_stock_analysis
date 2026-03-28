# -*- coding: utf-8 -*-
"""Unit tests for trade simulation decision logic."""

import unittest

from src.core.trade_simulation_engine import (
    compute_planned_quantity,
    infer_lot_size,
    infer_trade_action,
)


class TradeSimulationEngineTestCase(unittest.TestCase):
    def test_infer_trade_action_buy_sell_hold(self) -> None:
        self.assertEqual(infer_trade_action("买入", 0), "buy")
        self.assertEqual(infer_trade_action("卖出", 100), "sell")
        self.assertEqual(infer_trade_action("卖出", 0), "hold")
        self.assertEqual(infer_trade_action("持有", 100), "hold")

    def test_compute_buy_quantity_with_lot(self) -> None:
        qty = compute_planned_quantity(
            action="buy",
            price=20.0,
            budget=1000.0,
            holding_quantity=0,
            sell_fraction=0.5,
            lot_size=1,
        )
        self.assertEqual(qty, 50)

    def test_compute_partial_sell_quantity(self) -> None:
        qty = compute_planned_quantity(
            action="sell",
            price=10.0,
            budget=0.0,
            holding_quantity=100,
            sell_fraction=0.5,
            lot_size=1,
        )
        self.assertEqual(qty, 50)

    def test_infer_lot_size(self) -> None:
        self.assertEqual(infer_lot_size("600519"), 100)
        self.assertEqual(infer_lot_size("hk00700"), 100)
        self.assertEqual(infer_lot_size("AAPL"), 1)


if __name__ == "__main__":
    unittest.main()

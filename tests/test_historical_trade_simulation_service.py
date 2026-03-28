# -*- coding: utf-8 -*-
"""Deterministic tests for historical trade simulation service."""

from datetime import date
from dataclasses import dataclass
import sys
import types
from types import SimpleNamespace
import unittest

# Keep this test deterministic in minimal environments.
if "dotenv" not in sys.modules:
    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.load_dotenv = lambda *args, **kwargs: None
    dotenv_stub.dotenv_values = lambda *args, **kwargs: {}
    sys.modules["dotenv"] = dotenv_stub

from src.services.historical_trade_simulation_service import HistoricalTradeSimulationService


@dataclass
class _Bar:
    date: date
    open: float
    high: float
    low: float
    close: float


class _FakeStockRepo:
    def __init__(self, bars_by_code):
        self.bars_by_code = bars_by_code

    def get_range(self, code, start_date, end_date):
        rows = self.bars_by_code.get(code, [])
        return [r for r in rows if start_date <= r.date <= end_date]


class HistoricalTradeSimulationServiceTestCase(unittest.TestCase):
    def _make_bars(self):
        return {
            "AAPL": [
                _Bar(date=date(2025, 10, 1), open=100.0, high=102.0, low=99.0, close=100.0),
                _Bar(date=date(2025, 10, 2), open=101.0, high=103.0, low=100.0, close=102.0),
                _Bar(date=date(2025, 10, 3), open=103.0, high=104.0, low=101.0, close=103.0),
            ]
        }

    def test_close_mode_generates_equity_curve(self):
        bars = self._make_bars()

        def analyze(code, current_date):
            if current_date == date(2025, 10, 1):
                return {"name": code, "operation_advice": "买入"}
            if current_date == date(2025, 10, 2):
                return {"name": code, "operation_advice": "持有"}
            return {"name": code, "operation_advice": "卖出"}

        service = HistoricalTradeSimulationService(
            config=SimpleNamespace(agent_mode=False, agent_simulation_date=None),
            stock_repo=_FakeStockRepo(bars),
            analyze_callable=analyze,
        )
        output = service.run(
            stock_codes=["AAPL"],
            start_date=date(2025, 10, 1),
            end_date=date(2025, 10, 3),
            initial_cash=1000.0,
            execution_price_mode="close",
            sell_fraction=0.5,
            report_type="simple",
        )

        self.assertEqual(len(output.equity_curve), 3)
        self.assertGreater(len(output.decisions), 0)
        self.assertIn("final_equity", output.summary)

    def test_next_open_mode_queues_then_executes(self):
        bars = self._make_bars()

        def analyze(code, current_date):
            return {"name": code, "operation_advice": "买入"}

        service = HistoricalTradeSimulationService(
            config=SimpleNamespace(agent_mode=False, agent_simulation_date=None),
            stock_repo=_FakeStockRepo(bars),
            analyze_callable=analyze,
        )
        output = service.run(
            stock_codes=["AAPL"],
            start_date=date(2025, 10, 1),
            end_date=date(2025, 10, 3),
            initial_cash=300.0,
            execution_price_mode="next_open",
            sell_fraction=0.5,
            report_type="simple",
        )

        statuses = [row.get("status") for row in output.decisions]
        self.assertIn("queued_for_next_open", statuses)
        self.assertIn("filled", statuses)


if __name__ == "__main__":
    unittest.main()

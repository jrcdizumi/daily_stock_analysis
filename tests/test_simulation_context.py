# -*- coding: utf-8 -*-
"""Tests for Agent historical simulation helpers."""

from datetime import date
import unittest

import pandas as pd

from src.agent.simulation_context import truncate_ohlcv_dataframe


class TruncateOhlcvTests(unittest.TestCase):
    def test_truncate_keeps_on_or_before_as_of(self) -> None:
        df = pd.DataFrame(
            {
                "date": ["2024-01-02", "2024-06-15", "2024-12-30"],
                "close": [1.0, 2.0, 3.0],
            }
        )
        out = truncate_ohlcv_dataframe(df, date(2024, 6, 15))
        self.assertEqual(len(out), 2)
        self.assertListEqual(list(out["close"]), [1.0, 2.0])

    def test_empty_passthrough(self) -> None:
        self.assertIsNone(truncate_ohlcv_dataframe(None, date(2024, 1, 1)))


if __name__ == "__main__":
    unittest.main()

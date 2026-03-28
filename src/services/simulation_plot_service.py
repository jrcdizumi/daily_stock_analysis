# -*- coding: utf-8 -*-
"""Generate simulation equity/PnL line chart."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


class SimulationPlotService:
    """Create a PNG chart from equity-curve rows."""

    def plot(self, *, output_dir: str, equity_curve: List[Dict[str, Any]]) -> str:
        if not equity_curve:
            raise ValueError("equity_curve is empty")

        import matplotlib.pyplot as plt

        dates = [datetime.fromisoformat(str(row["date"])) for row in equity_curve]
        total_equity = [float(row.get("total_equity") or 0.0) for row in equity_curve]
        cumulative_pnl = [float(row.get("cumulative_pnl") or 0.0) for row in equity_curve]

        fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

        axes[0].plot(dates, total_equity, color="#1f77b4", linewidth=2.0)
        axes[0].set_title("Historical Trade Simulation: Total Equity")
        axes[0].set_ylabel("Equity")
        axes[0].grid(alpha=0.25)

        axes[1].plot(dates, cumulative_pnl, color="#d62728", linewidth=2.0)
        axes[1].set_title("Cumulative PnL")
        axes[1].set_ylabel("PnL")
        axes[1].set_xlabel("Date")
        axes[1].grid(alpha=0.25)

        fig.autofmt_xdate()
        fig.tight_layout()

        target = Path(output_dir)
        target.mkdir(parents=True, exist_ok=True)
        chart_path = target / "equity_curve.png"
        fig.savefig(chart_path, dpi=160)
        plt.close(fig)
        return str(chart_path)

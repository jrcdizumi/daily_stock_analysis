# -*- coding: utf-8 -*-
"""Export simulation artifacts to CSV/JSON files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


class SimulationExportService:
    """Persist simulation rows to disk for later analysis."""

    def export(
        self,
        *,
        output_dir: str,
        decisions: List[Dict[str, Any]],
        positions: List[Dict[str, Any]],
        equity_curve: List[Dict[str, Any]],
        summary: Dict[str, Any],
    ) -> Dict[str, str]:
        target = Path(output_dir)
        target.mkdir(parents=True, exist_ok=True)

        decisions_path = target / "decisions.csv"
        positions_path = target / "positions.csv"
        equity_path = target / "equity_curve.csv"
        summary_path = target / "summary.json"

        self._write_csv(decisions_path, decisions)
        self._write_csv(positions_path, positions)
        self._write_csv(equity_path, equity_curve)
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

        return {
            "decisions_csv": str(decisions_path),
            "positions_csv": str(positions_path),
            "equity_curve_csv": str(equity_path),
            "summary_json": str(summary_path),
        }

    def _write_csv(self, path: Path, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            path.write_text("", encoding="utf-8")
            return

        headers = list(rows[0].keys())
        lines = [",".join(headers)]
        for row in rows:
            values = []
            for key in headers:
                val = row.get(key)
                if val is None:
                    text = ""
                else:
                    text = str(val)
                # Escape commas/quotes/newlines for CSV safety.
                if any(ch in text for ch in [",", "\"", "\n", "\r"]):
                    text = '"' + text.replace('"', '""') + '"'
                values.append(text)
            lines.append(",".join(values))
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

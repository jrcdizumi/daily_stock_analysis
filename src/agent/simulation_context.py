# -*- coding: utf-8 -*-
"""
Historical simulation (as-of) scope for Agent tool calls.

When ``agent_simulation_date`` is set, tools must not use prices, news, or
fundamentals that would only be known after that calendar date.  This module
uses a context variable so handlers can read the cutoff without threading
dates through every tool signature.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar, Token
from datetime import date
from typing import Iterator, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_simulation_as_of: ContextVar[Optional[date]] = ContextVar("agent_simulation_as_of", default=None)


def get_simulation_as_of() -> Optional[date]:
    """Return the active simulation cutoff date, or None for live runs."""
    return _simulation_as_of.get()


@contextmanager
def simulation_as_of_scope(as_of: Optional[date]) -> Iterator[None]:
    """
    Bind ``as_of`` for the duration of the wrapped block (nested-safe).

    Pass None to clear the binding for nested live segments.
    """
    token: Token = _simulation_as_of.set(as_of)
    try:
        yield
    finally:
        _simulation_as_of.reset(token)


def truncate_ohlcv_dataframe(df: Optional[pd.DataFrame], as_of: date) -> Optional[pd.DataFrame]:
    """Keep only rows whose ``date`` column is on or before ``as_of``."""
    if df is None or df.empty or as_of is None:
        return df
    if "date" not in df.columns:
        return df
    try:
        parsed = pd.to_datetime(df["date"], errors="coerce").dt.date
        out = df.loc[parsed <= as_of].copy()
        return out
    except Exception as exc:
        logger.debug("truncate_ohlcv_dataframe failed: %s", exc)
        return df


def simulation_tool_blocked(
    *,
    reason_zh: str,
    reason_en: str = "Unavailable in historical simulation mode.",
) -> dict:
    """Standard response when a tool cannot run without look-ahead."""
    return {
        "error": reason_zh,
        "retriable": False,
        "simulation_blocked": True,
        "note_en": reason_en,
    }

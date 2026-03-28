# -*- coding: utf-8 -*-
"""Pure trade-simulation decision logic."""

from __future__ import annotations

from dataclasses import dataclass
from math import floor

from src.core.backtest_engine import BacktestEngine


@dataclass(frozen=True)
class DecisionResult:
    action: str  # buy/sell/hold
    planned_quantity: int


def infer_trade_action(operation_advice: str, holding_quantity: int) -> str:
    """Map operation advice text into buy/sell/hold (long-only)."""
    position = BacktestEngine.infer_position_recommendation(operation_advice)
    direction = BacktestEngine.infer_direction_expected(operation_advice)

    if position == "cash":
        return "sell" if holding_quantity > 0 else "hold"
    if direction == "up":
        return "buy"
    return "hold"


def infer_lot_size(stock_code: str) -> int:
    """Infer lot size by code convention; fallback to 1 share."""
    code = (stock_code or "").strip().lower()
    if code.startswith("hk"):
        return 100
    if code.startswith("sh") or code.startswith("sz") or code.startswith("bj"):
        return 100
    if len(code) == 6 and code.isdigit():
        return 100
    return 1


def compute_planned_quantity(
    *,
    action: str,
    price: float,
    budget: float,
    holding_quantity: int,
    sell_fraction: float,
    lot_size: int,
) -> int:
    """Compute integer order quantity under long-only constraints."""
    if lot_size <= 0:
        lot_size = 1

    if action == "buy":
        if price <= 0 or budget <= 0:
            return 0
        units = floor((budget / price) / lot_size)
        return max(0, int(units * lot_size))

    if action == "sell":
        if holding_quantity <= 0:
            return 0
        base_qty = int(holding_quantity * max(0.0, min(1.0, sell_fraction)))
        if base_qty <= 0:
            base_qty = min(holding_quantity, lot_size)
        lots = floor(base_qty / lot_size)
        qty = int(lots * lot_size)
        if qty <= 0:
            qty = min(holding_quantity, lot_size)
        return min(holding_quantity, qty)

    return 0


def make_decision(
    *,
    operation_advice: str,
    stock_code: str,
    price: float,
    budget: float,
    holding_quantity: int,
    sell_fraction: float,
) -> DecisionResult:
    """Single-stock decision helper for simulation workflows."""
    action = infer_trade_action(operation_advice=operation_advice, holding_quantity=holding_quantity)
    lot_size = infer_lot_size(stock_code)
    planned_quantity = compute_planned_quantity(
        action=action,
        price=price,
        budget=budget,
        holding_quantity=holding_quantity,
        sell_fraction=sell_fraction,
        lot_size=lot_size,
    )
    return DecisionResult(action=action, planned_quantity=planned_quantity)

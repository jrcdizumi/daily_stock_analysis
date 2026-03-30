# -*- coding: utf-8 -*-
"""Historical daily trade simulation service."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Literal, Optional

try:
    from data_provider.base import canonical_stock_code
except Exception:  # pragma: no cover - fallback for minimal test/runtime env
    def canonical_stock_code(code: str) -> str:
        return (code or "").strip().upper()

from src.config import Config, get_config
from src.core.trade_simulation_engine import make_decision
from src.enums import ReportType

logger = logging.getLogger(__name__)

ExecutionPriceMode = Literal["close", "next_open"]


@dataclass
class SimulationOutput:
    decisions: List[Dict[str, Any]]
    positions: List[Dict[str, Any]]
    equity_curve: List[Dict[str, Any]]
    summary: Dict[str, Any]


class HistoricalTradeSimulationService:
    """Run daily analysis + order simulation for a historical date range."""

    def __init__(
        self,
        *,
        config: Optional[Config] = None,
        stock_repo: Optional[Any] = None,
        analyze_callable: Optional[Callable[[str, date], Dict[str, Any]]] = None,
    ) -> None:
        self.config = config or get_config()
        if stock_repo is not None:
            self.stock_repo = stock_repo
        else:
            from src.repositories.stock_repo import StockRepository

            self.stock_repo = StockRepository()
        self._analyze_callable = analyze_callable
        self._fetcher_manager = None

    def run(
        self,
        *,
        stock_codes: List[str],
        start_date: date,
        end_date: date,
        initial_cash: float,
        execution_price_mode: ExecutionPriceMode,
        sell_fraction: float,
        report_type: ReportType,
    ) -> SimulationOutput:
        if start_date > end_date:
            raise ValueError("start_date must be <= end_date")
        if initial_cash <= 0:
            raise ValueError("initial_cash must be > 0")
        if execution_price_mode not in ("close", "next_open"):
            raise ValueError("execution_price_mode must be close or next_open")

        normalized_codes = [canonical_stock_code(code) for code in stock_codes if (code or "").strip()]
        normalized_codes = [code for code in normalized_codes if code]
        if not normalized_codes:
            raise ValueError("stock_codes is required")

        price_map = self._load_price_map(normalized_codes, start_date, end_date)
        trading_dates = self._build_trading_dates(price_map, start_date, end_date)
        if not trading_dates:
            raise ValueError("No historical bars found in the selected range")

        cash = float(initial_cash)
        holdings: Dict[str, int] = {code: 0 for code in normalized_codes}

        decisions: List[Dict[str, Any]] = []
        positions: List[Dict[str, Any]] = []
        equity_curve: List[Dict[str, Any]] = []
        pending_orders: List[Dict[str, Any]] = []

        original_agent_mode = getattr(self.config, "agent_mode", False)
        original_sim_date = getattr(self.config, "agent_simulation_date", None)

        pipeline: Optional[Any] = None
        if self._analyze_callable is None:
            from src.core.pipeline import StockAnalysisPipeline

            pipeline = StockAnalysisPipeline(
                config=self.config,
                max_workers=1,
                query_id=uuid.uuid4().hex,
                query_source="trade_simulation",
                save_context_snapshot=False,
            )

        try:
            self.config.agent_mode = True

            for current_date in trading_dates:
                self.config.agent_simulation_date = current_date.isoformat()

                if execution_price_mode == "next_open" and pending_orders:
                    cash = self._execute_pending_at_open(
                        pending_orders=pending_orders,
                        current_date=current_date,
                        price_map=price_map,
                        cash=cash,
                        holdings=holdings,
                        decisions=decisions,
                    )

                analyses: Dict[str, Dict[str, Any]] = {}
                for code in normalized_codes:
                    analyses[code] = self._analyze_one(
                        code=code,
                        current_date=current_date,
                        pipeline=pipeline,
                        report_type=report_type,
                    )

                buy_codes = []
                for code in normalized_codes:
                    price = self._get_price_for_decision(price_map, code, current_date, execution_price_mode)
                    if price is None or price <= 0:
                        continue
                    advice = str(analyses.get(code, {}).get("operation_advice") or "持有")
                    action = make_decision(
                        operation_advice=advice,
                        stock_code=code,
                        price=price,
                        budget=0.0,
                        holding_quantity=holdings.get(code, 0),
                        sell_fraction=sell_fraction,
                    ).action
                    if action == "buy":
                        buy_codes.append(code)

                buy_budget = cash / max(1, len(buy_codes)) if buy_codes else 0.0

                today_orders: List[Dict[str, Any]] = []
                for code in normalized_codes:
                    advice = str(analyses.get(code, {}).get("operation_advice") or "持有")
                    stock_name = str(analyses.get(code, {}).get("name") or code)
                    price = self._get_price_for_decision(price_map, code, current_date, execution_price_mode)

                    if price is None or price <= 0:
                        decisions.append(
                            {
                                "signal_date": current_date.isoformat(),
                                "execution_date": "",
                                "code": code,
                                "name": stock_name,
                                "operation_advice": advice,
                                "action": "hold",
                                "planned_quantity": 0,
                                "executed_quantity": 0,
                                "executed_price": None,
                                "status": "no_price",
                                "cash_after": round(cash, 6),
                                "position_after": holdings.get(code, 0),
                            }
                        )
                        continue

                    decision = make_decision(
                        operation_advice=advice,
                        stock_code=code,
                        price=price,
                        budget=buy_budget if code in buy_codes else 0.0,
                        holding_quantity=holdings.get(code, 0),
                        sell_fraction=sell_fraction,
                    )

                    order = {
                        "signal_date": current_date,
                        "code": code,
                        "name": stock_name,
                        "operation_advice": advice,
                        "action": decision.action,
                        "planned_quantity": int(decision.planned_quantity),
                    }

                    if execution_price_mode == "close":
                        trade_result = self._execute_order(
                            order=order,
                            execution_date=current_date,
                            execution_price=price,
                            cash=cash,
                            holdings=holdings,
                        )
                        cash = trade_result["cash"]
                        decisions.append(trade_result["decision_row"])
                    else:
                        today_orders.append(order)
                        decisions.append(
                            {
                                "signal_date": current_date.isoformat(),
                                "execution_date": "",
                                "code": code,
                                "name": stock_name,
                                "operation_advice": advice,
                                "action": order["action"],
                                "planned_quantity": order["planned_quantity"],
                                "executed_quantity": 0,
                                "executed_price": None,
                                "status": "queued_for_next_open",
                                "cash_after": round(cash, 6),
                                "position_after": holdings.get(code, 0),
                            }
                        )

                if execution_price_mode == "next_open":
                    pending_orders = today_orders

                self._record_positions_and_equity(
                    current_date=current_date,
                    stock_codes=normalized_codes,
                    holdings=holdings,
                    cash=cash,
                    price_map=price_map,
                    positions=positions,
                    equity_curve=equity_curve,
                )

            if execution_price_mode == "next_open" and pending_orders:
                for order in pending_orders:
                    decisions.append(
                        {
                            "signal_date": order["signal_date"].isoformat(),
                            "execution_date": "",
                            "code": order["code"],
                            "name": order["name"],
                            "operation_advice": order["operation_advice"],
                            "action": order["action"],
                            "planned_quantity": order["planned_quantity"],
                            "executed_quantity": 0,
                            "executed_price": None,
                            "status": "pending_dropped_out_of_range",
                            "cash_after": round(cash, 6),
                            "position_after": holdings.get(order["code"], 0),
                        }
                    )

        finally:
            self.config.agent_mode = original_agent_mode
            self.config.agent_simulation_date = original_sim_date

        final_equity = float(equity_curve[-1]["total_equity"]) if equity_curve else float(initial_cash)
        summary = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "stock_codes": normalized_codes,
            "initial_cash": round(float(initial_cash), 6),
            "final_cash": round(float(cash), 6),
            "final_equity": round(final_equity, 6),
            "pnl": round(final_equity - float(initial_cash), 6),
            "pnl_pct": round((final_equity / float(initial_cash) - 1.0) * 100.0, 6),
            "trading_days": len(trading_dates),
            "execution_price_mode": execution_price_mode,
            "sell_fraction": round(float(sell_fraction), 6),
        }

        return SimulationOutput(
            decisions=decisions,
            positions=positions,
            equity_curve=equity_curve,
            summary=summary,
        )

    def _load_price_map(
        self,
        stock_codes: List[str],
        start_date: date,
        end_date: date,
    ) -> Dict[str, Dict[date, Any]]:
        price_map: Dict[str, Dict[date, Any]] = {}
        ext_end = end_date + timedelta(days=7)

        for code in stock_codes:
            bars = self.stock_repo.get_range(code, start_date, ext_end)
            
            if not bars:
                logger.info(f"{code} 数据库中无数据，尝试从数据源获取...")
                self._fetch_and_save_data(code, start_date, ext_end)
                bars = self.stock_repo.get_range(code, start_date, ext_end)
            
            price_map[code] = {bar.date: bar for bar in bars}
        return price_map

    def _build_trading_dates(
        self,
        price_map: Dict[str, Dict[date, Any]],
        start_date: date,
        end_date: date,
    ) -> List[date]:
        days = set()
        for by_date in price_map.values():
            for d in by_date.keys():
                if start_date <= d <= end_date:
                    days.add(d)
        return sorted(days)

    def _analyze_one(
        self,
        *,
        code: str,
        current_date: date,
        pipeline: Optional[Any],
        report_type: ReportType,
    ) -> Dict[str, Any]:
        if self._analyze_callable is not None:
            payload = self._analyze_callable(code, current_date) or {}
            return {
                "name": payload.get("name") or code,
                "operation_advice": payload.get("operation_advice") or "持有",
            }

        assert pipeline is not None
        query_id = f"sim-{current_date.isoformat()}-{code}-{uuid.uuid4().hex[:8]}"
        result = pipeline.analyze_stock(code, report_type, query_id=query_id)
        if result is None:
            return {"name": code, "operation_advice": "持有"}
        return {
            "name": result.name,
            "operation_advice": result.operation_advice,
        }

    def _get_price_for_decision(
        self,
        price_map: Dict[str, Dict[date, Any]],
        code: str,
        current_date: date,
        execution_price_mode: ExecutionPriceMode,
    ) -> Optional[float]:
        bar = price_map.get(code, {}).get(current_date)
        if bar is None:
            return None
        if execution_price_mode == "close":
            return float(bar.close) if bar.close is not None else None
        return float(bar.close) if bar.close is not None else None

    def _execute_pending_at_open(
        self,
        *,
        pending_orders: List[Dict[str, Any]],
        current_date: date,
        price_map: Dict[str, Dict[date, Any]],
        cash: float,
        holdings: Dict[str, int],
        decisions: List[Dict[str, Any]],
    ) -> float:
        for order in pending_orders:
            bar = price_map.get(order["code"], {}).get(current_date)
            if bar is None or bar.open is None or float(bar.open) <= 0:
                decisions.append(
                    {
                        "signal_date": order["signal_date"].isoformat(),
                        "execution_date": current_date.isoformat(),
                        "code": order["code"],
                        "name": order["name"],
                        "operation_advice": order["operation_advice"],
                        "action": order["action"],
                        "planned_quantity": order["planned_quantity"],
                        "executed_quantity": 0,
                        "executed_price": None,
                        "status": "execution_price_missing",
                        "cash_after": round(cash, 6),
                        "position_after": holdings.get(order["code"], 0),
                    }
                )
                continue

            trade_result = self._execute_order(
                order=order,
                execution_date=current_date,
                execution_price=float(bar.open),
                cash=cash,
                holdings=holdings,
            )
            cash = trade_result["cash"]
            decisions.append(trade_result["decision_row"])

        return cash

    def _execute_order(
        self,
        *,
        order: Dict[str, Any],
        execution_date: date,
        execution_price: float,
        cash: float,
        holdings: Dict[str, int],
    ) -> Dict[str, Any]:
        code = order["code"]
        action = order["action"]
        planned = int(order["planned_quantity"])
        executed = 0
        status = "no_op"

        if action == "buy" and planned > 0 and execution_price > 0:
            affordable = int(cash // execution_price)
            executed = min(planned, max(0, affordable))
            if executed > 0:
                cash -= executed * execution_price
                holdings[code] = int(holdings.get(code, 0) + executed)
                status = "filled"
            else:
                status = "insufficient_cash"
        elif action == "sell" and planned > 0 and execution_price > 0:
            available = int(holdings.get(code, 0))
            executed = min(planned, max(0, available))
            if executed > 0:
                cash += executed * execution_price
                holdings[code] = int(max(0, available - executed))
                status = "filled"
            else:
                status = "no_position"

        decision_row = {
            "signal_date": order["signal_date"].isoformat(),
            "execution_date": execution_date.isoformat(),
            "code": code,
            "name": order["name"],
            "operation_advice": order["operation_advice"],
            "action": action,
            "planned_quantity": planned,
            "executed_quantity": executed,
            "executed_price": round(float(execution_price), 6),
            "status": status,
            "cash_after": round(float(cash), 6),
            "position_after": int(holdings.get(code, 0)),
        }
        return {"cash": cash, "decision_row": decision_row}

    def _record_positions_and_equity(
        self,
        *,
        current_date: date,
        stock_codes: List[str],
        holdings: Dict[str, int],
        cash: float,
        price_map: Dict[str, Dict[date, Any]],
        positions: List[Dict[str, Any]],
        equity_curve: List[Dict[str, Any]],
    ) -> None:
        total_market_value = 0.0
        for code in stock_codes:
            qty = int(holdings.get(code, 0))
            close_price = self._get_close_as_of(price_map.get(code, {}), current_date)
            market_value = float(qty * close_price) if close_price is not None else 0.0
            total_market_value += market_value
            positions.append(
                {
                    "date": current_date.isoformat(),
                    "code": code,
                    "quantity": qty,
                    "close_price": round(float(close_price), 6) if close_price is not None else None,
                    "market_value": round(float(market_value), 6),
                }
            )

        total_equity = float(cash) + total_market_value
        prev_equity = float(equity_curve[-1]["total_equity"]) if equity_curve else total_equity
        daily_pnl = total_equity - prev_equity if equity_curve else 0.0
        base_equity = float(equity_curve[0]["total_equity"]) if equity_curve else total_equity
        cumulative_pnl = total_equity - base_equity

        equity_curve.append(
            {
                "date": current_date.isoformat(),
                "cash": round(float(cash), 6),
                "market_value": round(float(total_market_value), 6),
                "total_equity": round(float(total_equity), 6),
                "daily_pnl": round(float(daily_pnl), 6),
                "cumulative_pnl": round(float(cumulative_pnl), 6),
            }
        )

    def _get_close_as_of(self, bars_by_date: Dict[date, Any], current_date: date) -> Optional[float]:
        dates = [d for d in bars_by_date.keys() if d <= current_date]
        if not dates:
            return None
        last_day = max(dates)
        bar = bars_by_date[last_day]
        return float(bar.close) if bar.close is not None else None

    def _fetch_and_save_data(self, code: str, start_date: date, end_date: date) -> None:
        """从数据源获取历史数据并保存到数据库
        
        为了计算技术指标（MA30、MACD等），会往前多获取60天数据
        """
        if self._fetcher_manager is None:
            from data_provider import DataFetcherManager
            self._fetcher_manager = DataFetcherManager()
        
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager.get_instance()
            
            fetch_start = start_date - timedelta(days=60)
            days_needed = (end_date - fetch_start).days
            logger.info(f"从数据源获取 {code} 的历史数据（{fetch_start.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}，约 {days_needed} 天，额外60天用于计算技术指标）...")
            
            df, source_name = self._fetcher_manager.get_daily_data(
                code,
                start_date=fetch_start.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )
            
            if df is None or df.empty:
                logger.warning(f"{code} 从数据源获取数据失败")
                return
            
            saved_count = db.save_daily_data(df, code, source_name)
            logger.info(f"{code} 历史数据保存成功（来源: {source_name}，新增 {saved_count} 条）")
        except Exception as e:
            logger.warning(f"{code} 获取历史数据失败: {e}")

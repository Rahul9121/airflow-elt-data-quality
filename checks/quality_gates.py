"""Custom data quality gates for trade batches."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]


def load_config(root: Path | None = None) -> dict[str, Any]:
    base = root or ROOT
    return yaml.safe_load((base / "config" / "pipeline.yaml").read_text(encoding="utf-8"))


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str
    metric: float | int | None = None
    threshold: float | int | str | None = None


@dataclass
class GateReport:
    trade_date: str
    passed: bool
    checks: list[CheckResult] = field(default_factory=list)
    raw_trade_count: int = 0
    processed_trade_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade_date": self.trade_date,
            "passed": self.passed,
            "raw_trade_count": self.raw_trade_count,
            "processed_trade_count": self.processed_trade_count,
            "checks": [asdict(c) for c in self.checks],
        }


def check_schema(df: pd.DataFrame, expected: list[str]) -> CheckResult:
    cols = list(df.columns)
    missing = [c for c in expected if c not in cols]
    extra = [c for c in cols if c not in expected]
    ok = not missing and not extra
    detail = f"missing={missing} extra={extra}"
    return CheckResult("schema_drift", ok, detail, metric=len(missing) + len(extra), threshold=0)


def check_null_rates(df: pd.DataFrame, cols: list[str], max_rate: float) -> CheckResult:
    rates = {}
    worst = 0.0
    for c in cols:
        if c not in df.columns:
            rates[c] = 1.0
            worst = 1.0
            continue
        rate = float(df[c].isna().mean()) if len(df) else 1.0
        rates[c] = round(rate, 4)
        worst = max(worst, rate)
    ok = worst <= max_rate
    return CheckResult(
        "null_rates",
        ok,
        f"rates={rates}",
        metric=round(worst, 4),
        threshold=max_rate,
    )


def check_value_ranges(
    df: pd.DataFrame,
    price_min: float,
    price_max: float,
    qty_min: int,
    qty_max: int,
    allowed_sides: list[str],
) -> CheckResult:
    issues = []
    if "price" in df.columns:
        bad_p = df["price"].dropna()
        n_bad_p = int(((bad_p < price_min) | (bad_p > price_max)).sum())
        if n_bad_p:
            issues.append(f"price_out_of_range={n_bad_p}")
    if "quantity" in df.columns:
        bad_q = df["quantity"].dropna()
        n_bad_q = int(((bad_q < qty_min) | (bad_q > qty_max)).sum())
        if n_bad_q:
            issues.append(f"quantity_out_of_range={n_bad_q}")
    if "side" in df.columns:
        n_bad_s = int((~df["side"].isin(allowed_sides) & df["side"].notna()).sum())
        if n_bad_s:
            issues.append(f"invalid_side={n_bad_s}")
    ok = not issues
    return CheckResult(
        "value_ranges",
        ok,
        "; ".join(issues) if issues else "all values in range",
        metric=len(issues),
        threshold=0,
    )


def check_referential_integrity(trades: pd.DataFrame, accounts: pd.DataFrame) -> CheckResult:
    if "account_id" not in trades.columns or "account_id" not in accounts.columns:
        return CheckResult("referential_integrity", False, "account_id missing", metric=1, threshold=0)
    valid = set(accounts["account_id"].dropna().astype(str))
    orphan = trades["account_id"].dropna().astype(str)
    n_orphan = int((~orphan.isin(valid)).sum())
    ok = n_orphan == 0
    return CheckResult(
        "referential_integrity",
        ok,
        f"orphan_account_rows={n_orphan}",
        metric=n_orphan,
        threshold=0,
    )


def check_row_count(n: int, lo: int, hi: int) -> CheckResult:
    ok = lo <= n <= hi
    return CheckResult(
        "row_count",
        ok,
        f"count={n} expected=[{lo},{hi}]",
        metric=n,
        threshold=f"{lo}-{hi}",
    )


def check_reconciliation(raw_n: int, processed_n: int, allow_drop: int = 0) -> CheckResult:
    delta = abs(raw_n - processed_n)
    ok = delta <= allow_drop
    return CheckResult(
        "row_count_reconciliation",
        ok,
        f"raw={raw_n} processed={processed_n} delta={delta}",
        metric=delta,
        threshold=allow_drop,
    )


def run_gates(
    trade_date: str,
    trades: pd.DataFrame,
    accounts: pd.DataFrame,
    processed: pd.DataFrame | None = None,
    cfg: dict[str, Any] | None = None,
) -> GateReport:
    cfg = cfg or load_config()
    c = cfg["checks"]
    expected = cfg["schema"]["trades"]
    checks = [
        check_schema(trades, expected),
        check_null_rates(trades, expected, c["null_rate_max"]),
        check_value_ranges(
            trades,
            c["price_min"],
            c["price_max"],
            c["quantity_min"],
            c["quantity_max"],
            c["allowed_sides"],
        ),
        check_referential_integrity(trades, accounts),
        check_row_count(len(trades), c["row_count_min"], c["row_count_max"]),
    ]
    proc = processed if processed is not None else trades
    # For good batches spark may filter nothing; reconcile against processed length
    checks.append(check_reconciliation(len(trades), len(proc), allow_drop=0 if checks[0].passed else len(trades)))
    report = GateReport(
        trade_date=trade_date,
        passed=all(ch.passed for ch in checks),
        checks=checks,
        raw_trade_count=len(trades),
        processed_trade_count=len(proc),
    )
    return report

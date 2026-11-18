from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "dags"))

from checks.quality_gates import (  # noqa: E402
    check_null_rates,
    check_referential_integrity,
    check_schema,
    check_value_ranges,
    run_gates,
)
from scripts.seed_data import build_accounts, build_trades  # noqa: E402


def test_good_batch_passes_gates():
    trades = build_trades("2024-06-01", bad=False)
    accounts = build_accounts()
    report = run_gates("2024-06-01", trades, accounts, processed=trades)
    assert report.passed is True
    assert report.raw_trade_count == 180


def test_bad_batch_fails_multiple_checks():
    trades = build_trades("2024-06-03", bad=True)
    accounts = build_accounts()
    report = run_gates("2024-06-03", trades, accounts, processed=trades)
    assert report.passed is False
    failed = {c.name for c in report.checks if not c.passed}
    assert "schema_drift" in failed
    assert "null_rates" in failed
    assert "value_ranges" in failed
    assert "referential_integrity" in failed


def test_schema_detects_extra_column():
    df = pd.DataFrame({"a": [1], "b": [2]})
    r = check_schema(df, ["a"])
    assert r.passed is False
    assert "b" in r.detail


def test_null_rate_threshold():
    df = pd.DataFrame({"a": [1, None, None, None]})
    r = check_null_rates(df, ["a"], max_rate=0.2)
    assert r.passed is False


def test_referential_orphans():
    trades = pd.DataFrame({"account_id": ["A1", "MISSING"]})
    accounts = pd.DataFrame({"account_id": ["A1"]})
    r = check_referential_integrity(trades, accounts)
    assert r.passed is False
    assert r.metric == 1


def test_value_ranges_side():
    df = pd.DataFrame({"price": [10.0], "quantity": [5], "side": ["HOLD"]})
    r = check_value_ranges(df, 0.01, 1000, 1, 100, ["BUY", "SELL"])
    assert r.passed is False


def test_dag_integrity_import():
    pytest.importorskip("airflow")
    from airflow.models import DagBag

    dag_bag = DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)
    assert dag_bag.import_errors == {}
    assert "trade_elt_quality_gates" in dag_bag.dags
    dag = dag_bag.dags["trade_elt_quality_gates"]
    assert dag.default_args.get("retries") == 2
    task_ids = dag.task_ids
    for needed in [
        "wait_for_raw_batch",
        "ingest_batch",
        "spark_transform",
        "dbt_build",
        "run_quality_gates",
        "branch_on_gates",
        "quarantine_and_alert",
        "publish_report",
    ]:
        assert needed in task_ids

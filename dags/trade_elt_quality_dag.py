"""Daily trade ELT with quality gates (TaskFlow API)."""
from __future__ import annotations

import json
import logging
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
import yaml
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# Project root: repo root when running locally / in container
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "dags") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "dags"))

CFG = yaml.safe_load((PROJECT_ROOT / "config" / "pipeline.yaml").read_text(encoding="utf-8"))


def _fail_callback(context):
    ti = context.get("task_instance")
    log.error("TASK FAILED: %s dag=%s run=%s", ti.task_id if ti else "?", context.get("dag"), context.get("run_id"))
    alert_dir = PROJECT_ROOT / CFG["paths"]["alerts"]
    alert_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "trade_date": str(context.get("data_interval_start") or context.get("logical_date")),
        "message": f"failure_callback task={getattr(ti, 'task_id', None)}",
        "ts": datetime.utcnow().isoformat() + "Z",
    }
    (alert_dir / f"fail_{getattr(ti, 'task_id', 'unknown')}_{payload['ts'].replace(':', '')}.json").write_text(
        json.dumps(payload, indent=2), encoding="utf-8"
    )


default_args = {
    "owner": "rahul-ega",
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": _fail_callback,
}


@dag(
    dag_id="trade_elt_quality_gates",
    description="Ingest trades, PySpark transform, dbt on DuckDB, quality gates, report or quarantine",
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    end_date=pendulum.datetime(2024, 6, 6, tz="UTC"),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["elt", "quality", "trades", "portfolio"],
    doc_md=__doc__,
    sla_miss_callback=None,
)
def trade_elt_quality_gates():
    start = EmptyOperator(task_id="start")

    wait_for_batch = FileSensor(
        task_id="wait_for_raw_batch",
        filepath=str(PROJECT_ROOT / "data" / "raw" / "trade_date={{ ds }}" / "_SUCCESS"),
        poke_interval=5,
        timeout=120,
        mode="poke",
        soft_fail=False,
    )

    @task(task_id="ingest_batch", retries=2)
    def ingest_batch(ds: str) -> dict:
        raw_dir = PROJECT_ROOT / "data" / "raw" / f"trade_date={ds}"
        trades = raw_dir / "trades.csv"
        accounts = raw_dir / "accounts.csv"
        if not trades.exists() or not accounts.exists():
            raise AirflowFailException(f"missing raw files for {ds}")
        staged = PROJECT_ROOT / "data" / "processed" / f"trade_date={ds}" / "raw"
        staged.mkdir(parents=True, exist_ok=True)
        shutil.copy2(trades, staged / "trades.csv")
        shutil.copy2(accounts, staged / "accounts.csv")
        return {"ds": ds, "trades": str(staged / "trades.csv"), "accounts": str(staged / "accounts.csv")}

    @task(task_id="spark_transform", execution_timeout=timedelta(minutes=15))
    def spark_transform(meta: dict) -> dict:
        from utils.spark_transform import transform_trades

        ds = meta["ds"]
        out = PROJECT_ROOT / "data" / "processed" / f"trade_date={ds}" / "trades.parquet"
        n = transform_trades(meta["trades"], str(out))
        meta = dict(meta)
        meta["parquet"] = str(out)
        meta["processed_count"] = n
        return meta

    @task(task_id="dbt_build")
    def dbt_build(meta: dict) -> dict:
        import duckdb
        import pandas as pd

        ds = meta["ds"]
        duck_path = PROJECT_ROOT / CFG["paths"]["duckdb"]
        duck_path.parent.mkdir(parents=True, exist_ok=True)
        trades = pd.read_parquet(meta["parquet"])
        accounts = pd.read_csv(meta["accounts"])
        con = duckdb.connect(str(duck_path))
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS staging")
            con.execute("CREATE SCHEMA IF NOT EXISTS marts")
            con.register("trades_raw", trades)
            con.register("accounts_raw", accounts)
            con.execute("CREATE OR REPLACE TABLE staging.stg_trades AS SELECT * FROM trades_raw")
            con.execute("CREATE OR REPLACE TABLE staging.stg_accounts AS SELECT * FROM accounts_raw")
            con.execute(
                """
                CREATE OR REPLACE TABLE marts.fct_trades AS
                SELECT
                  t.trade_id,
                  t.trade_date,
                  t.symbol,
                  t.side,
                  t.quantity,
                  t.price,
                  t.account_id,
                  t.venue,
                  t.notional,
                  a.account_name,
                  a.region,
                  a.tier
                FROM staging.stg_trades t
                LEFT JOIN staging.stg_accounts a USING (account_id)
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TABLE marts.agg_daily_symbol AS
                SELECT trade_date, symbol, side,
                       COUNT(*) AS trade_count,
                       SUM(quantity) AS total_qty,
                       SUM(notional) AS total_notional
                FROM marts.fct_trades
                GROUP BY 1,2,3
                """
            )
            # Write dbt-like manifest marker for the report/CI
            run_dir = PROJECT_ROOT / "dbt_project" / "target"
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "run_results.json").write_text(
                json.dumps({"ds": ds, "status": "success", "models": ["stg_trades", "fct_trades", "agg_daily_symbol"]}),
                encoding="utf-8",
            )
        finally:
            con.close()
        return meta

    @task(task_id="run_quality_gates", sla=timedelta(minutes=10))
    def run_quality_gates(meta: dict) -> dict:
        import pandas as pd
        from checks.quality_gates import run_gates

        ds = meta["ds"]
        raw_trades = pd.read_csv(meta["trades"])
        accounts = pd.read_csv(meta["accounts"])
        processed = pd.read_parquet(meta["parquet"])
        report = run_gates(ds, raw_trades, accounts, processed=processed, cfg=CFG)
        out_dir = PROJECT_ROOT / "data" / "processed" / "gate_results"
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{ds}.json"
        path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
        meta = dict(meta)
        meta["gate_passed"] = report.passed
        meta["gate_path"] = str(path)
        meta["failed_checks"] = [c.name for c in report.checks if not c.passed]
        return meta

    @task.branch(task_id="branch_on_gates")
    def branch_on_gates(meta: dict) -> str:
        if meta.get("gate_passed"):
            return "publish_report"
        return "quarantine_and_alert"

    @task(task_id="quarantine_and_alert")
    def quarantine_and_alert(meta: dict) -> dict:
        ds = meta["ds"]
        q_root = PROJECT_ROOT / CFG["paths"]["quarantine"] / f"trade_date={ds}"
        q_root.mkdir(parents=True, exist_ok=True)
        src = PROJECT_ROOT / "data" / "processed" / f"trade_date={ds}"
        if src.exists():
            for p in src.rglob("*"):
                if p.is_file():
                    dest = q_root / p.relative_to(src)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(p, dest)
        reason = "quality_gates_failed:" + ",".join(meta.get("failed_checks") or [])
        record = {"trade_date": ds, "reason": reason, "path": str(q_root)}
        (PROJECT_ROOT / CFG["paths"]["quarantine"] / f"{ds}.json").write_text(
            json.dumps(record, indent=2), encoding="utf-8"
        )
        alert = {
            "trade_date": ds,
            "message": f"Batch blocked by quality gates: {reason}",
            "ts": datetime.utcnow().isoformat() + "Z",
        }
        (PROJECT_ROOT / CFG["paths"]["alerts"] / f"{ds}.json").write_text(
            json.dumps(alert, indent=2), encoding="utf-8"
        )
        raise AirflowFailException(f"Quality gates failed for {ds}: {reason}")

    @task(task_id="publish_report")
    def publish_report(meta: dict) -> str:
        # Import generator
        sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
        from generate_report import main as gen

        gen()
        return str(PROJECT_ROOT / CFG["paths"]["report"])

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    meta = ingest_batch()
    transformed = spark_transform(meta)
    modeled = dbt_build(transformed)
    gated = run_quality_gates(modeled)
    branch = branch_on_gates(gated)
    q = quarantine_and_alert(gated)
    pub = publish_report(gated)

    start >> wait_for_batch >> meta
    branch >> [q, pub]
    [q, pub] >> end


trade_elt_quality_gates()

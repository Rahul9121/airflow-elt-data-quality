"""Run the ELT + gates for one date without the Airflow scheduler."""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "dags"))

from checks.quality_gates import run_gates  # noqa: E402
from utils.spark_transform import transform_trades  # noqa: E402

CFG = yaml.safe_load((ROOT / "config" / "pipeline.yaml").read_text(encoding="utf-8"))


def run_one(ds: str) -> dict:
    t0 = time.time()
    timings = {}
    raw_dir = ROOT / "data" / "raw" / f"trade_date={ds}"
    if not (raw_dir / "_SUCCESS").exists():
        raise SystemExit(f"missing raw batch marker for {ds}")

    t = time.time()
    staged = ROOT / "data" / "processed" / f"trade_date={ds}" / "raw"
    staged.mkdir(parents=True, exist_ok=True)
    shutil.copy2(raw_dir / "trades.csv", staged / "trades.csv")
    shutil.copy2(raw_dir / "accounts.csv", staged / "accounts.csv")
    timings["ingest_s"] = round(time.time() - t, 3)

    t = time.time()
    parquet = ROOT / "data" / "processed" / f"trade_date={ds}" / "trades.parquet"
    n = transform_trades(str(staged / "trades.csv"), str(parquet))
    timings["spark_s"] = round(time.time() - t, 3)

    t = time.time()
    import duckdb

    trades = pd.read_parquet(parquet)
    accounts = pd.read_csv(staged / "accounts.csv")
    duck_path = ROOT / CFG["paths"]["duckdb"]
    con = duckdb.connect(str(duck_path))
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute("CREATE SCHEMA IF NOT EXISTS marts")
    con.register("trades_raw", trades)
    con.register("accounts_raw", accounts)
    con.execute("CREATE OR REPLACE TABLE staging.stg_trades AS SELECT * FROM trades_raw")
    con.execute("CREATE OR REPLACE TABLE staging.stg_accounts AS SELECT * FROM accounts_raw")
    con.execute(
        """
        CREATE OR REPLACE TABLE marts.fct_trades AS
        SELECT t.*, a.account_name, a.region, a.tier
        FROM staging.stg_trades t
        LEFT JOIN staging.stg_accounts a USING (account_id)
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE marts.agg_daily_symbol AS
        SELECT trade_date, symbol, side, COUNT(*) AS trade_count,
               SUM(quantity) AS total_qty, SUM(notional) AS total_notional
        FROM marts.fct_trades GROUP BY 1,2,3
        """
    )
    con.close()
    timings["dbt_s"] = round(time.time() - t, 3)

    t = time.time()
    raw_trades = pd.read_csv(staged / "trades.csv")
    report = run_gates(ds, raw_trades, accounts, processed=trades, cfg=CFG)
    out_dir = ROOT / "data" / "processed" / "gate_results"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{ds}.json").write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    timings["gates_s"] = round(time.time() - t, 3)
    timings["total_s"] = round(time.time() - t0, 3)
    timings["processed_count"] = n

    if not report.passed:
        q_root = ROOT / CFG["paths"]["quarantine"] / f"trade_date={ds}"
        q_root.mkdir(parents=True, exist_ok=True)
        src = ROOT / "data" / "processed" / f"trade_date={ds}"
        for p in src.rglob("*"):
            if p.is_file():
                dest = q_root / p.relative_to(src)
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(p, dest)
        reason = "quality_gates_failed:" + ",".join(c.name for c in report.checks if not c.passed)
        (ROOT / CFG["paths"]["quarantine"] / f"{ds}.json").write_text(
            json.dumps({"trade_date": ds, "reason": reason, "path": str(q_root)}, indent=2),
            encoding="utf-8",
        )
        (ROOT / CFG["paths"]["alerts"] / f"{ds}.json").write_text(
            json.dumps(
                {
                    "trade_date": ds,
                    "message": f"Batch blocked by quality gates: {reason}",
                    "ts": datetime.utcnow().isoformat() + "Z",
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        print(json.dumps({"ds": ds, "passed": False, "timings": timings, "failed": [c.name for c in report.checks if not c.passed]}))
        raise SystemExit(2)

    print(json.dumps({"ds": ds, "passed": True, "timings": timings}))
    return report.to_dict()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--all-seed", action="store_true", help="Run all seed dates (bad day may exit 2)")
    args = p.parse_args()
    if args.all_seed:
        codes = []
        for d in CFG["seed_dates"]:
            try:
                run_one(d)
                codes.append(0)
            except SystemExit as e:
                codes.append(int(e.code or 1))
        # success if at least one good day passed and bad day failed
        raise SystemExit(0 if 0 in codes and 2 in codes else 1)
    run_one(args.date)


if __name__ == "__main__":
    main()

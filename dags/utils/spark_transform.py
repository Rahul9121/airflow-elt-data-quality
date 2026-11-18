"""Local-mode PySpark transform for daily trades."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

WANTED = [
    "trade_id",
    "trade_date",
    "symbol",
    "side",
    "quantity",
    "price",
    "account_id",
    "venue",
]


def _transform_pandas(raw_trades_csv: str, out_parquet: str) -> int:
    import pandas as pd

    pdf = pd.read_csv(raw_trades_csv)
    present = [c for c in WANTED if c in pdf.columns]
    pdf = pdf[present].copy()
    pdf = pdf[pdf["trade_id"].notna()]
    pdf["quantity"] = pd.to_numeric(pdf["quantity"], errors="coerce")
    pdf["price"] = pd.to_numeric(pdf["price"], errors="coerce")
    pdf["notional"] = pdf["quantity"] * pdf["price"]
    pdf["ingest_ts"] = datetime.now(timezone.utc)
    out = Path(out_parquet)
    out.parent.mkdir(parents=True, exist_ok=True)
    pdf.to_parquet(out_parquet, index=False)
    return len(pdf)


def transform_trades(raw_trades_csv: str, out_parquet: str) -> int:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except Exception:
        return _transform_pandas(raw_trades_csv, out_parquet)

    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("trade-elt")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        df = spark.read.option("header", True).option("inferSchema", True).csv(raw_trades_csv)
        present = [c for c in WANTED if c in df.columns]
        df = df.select(*present)
        df = df.filter(F.col("trade_id").isNotNull())
        df = df.withColumn("notional", F.col("quantity").cast("double") * F.col("price").cast("double"))
        df = df.withColumn("ingest_ts", F.current_timestamp())
        Path(out_parquet).parent.mkdir(parents=True, exist_ok=True)
        pdf = df.toPandas()
        pdf.to_parquet(out_parquet, index=False)
        spark.stop()
        return len(pdf)
    except Exception:
        try:
            spark.stop()
        except Exception:
            pass
        return _transform_pandas(raw_trades_csv, out_parquet)

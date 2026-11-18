-- Staging trades from the processed parquet registered externally.
-- In CI/local DAG we also materialize via DuckDB Python for reliability.
SELECT * FROM staging.stg_trades

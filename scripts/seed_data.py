"""Generate synthetic daily trade batches. One bad batch is intentional."""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
CFG = yaml.safe_load((ROOT / "config" / "pipeline.yaml").read_text(encoding="utf-8"))

SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "JPM", "XOM"]
VENUES = ["NYSE", "NASDAQ", "ARCA", "BATS"]
ACCOUNTS = [
    ("ACC001", "Alpha Capital", "US", "tier1"),
    ("ACC002", "Beta Funds", "US", "tier1"),
    ("ACC003", "Gamma Desk", "EU", "tier2"),
    ("ACC004", "Delta Prop", "APAC", "tier2"),
    ("ACC005", "Epsilon Retail", "US", "tier3"),
]


def _rng_seed(trade_date: str) -> int:
    return int(hashlib.md5(trade_date.encode()).hexdigest()[:8], 16)


def build_accounts() -> pd.DataFrame:
    return pd.DataFrame(ACCOUNTS, columns=CFG["schema"]["accounts"])


def build_trades(trade_date: str, bad: bool = False) -> pd.DataFrame:
    import random

    rng = random.Random(_rng_seed(trade_date))
    n = 180 if not bad else 160
    rows = []
    for i in range(n):
        rows.append(
            {
                "trade_id": f"T{trade_date.replace('-', '')}-{i:04d}",
                "trade_date": trade_date,
                "symbol": rng.choice(SYMBOLS),
                "side": rng.choice(["BUY", "SELL"]),
                "quantity": rng.randint(10, 5000),
                "price": round(rng.uniform(12.0, 980.0), 2),
                "account_id": rng.choice([a[0] for a in ACCOUNTS]),
                "venue": rng.choice(VENUES),
            }
        )
    df = pd.DataFrame(rows, columns=CFG["schema"]["trades"])

    if bad:
        # Schema drift: unexpected column
        df["extra_fee_code"] = "X"
        # Null spike on price
        df.loc[df.index[:25], "price"] = None
        # Out of range quantity and invalid side
        df.loc[df.index[30:35], "quantity"] = -5
        df.loc[df.index[40:45], "side"] = "HOLD"
        # Orphan account keys
        df.loc[df.index[50:60], "account_id"] = "ACC999"
        # Drop required column to amplify schema fail path in some checks
        # Keep venue but corrupt a few trade_ids to null
        df.loc[df.index[70:78], "trade_id"] = None
    return df


def write_batch(trade_date: str, out_dir: Path, bad: bool) -> None:
    day_dir = out_dir / f"trade_date={trade_date}"
    day_dir.mkdir(parents=True, exist_ok=True)
    trades = build_trades(trade_date, bad=bad)
    accounts = build_accounts()
    trades.to_csv(day_dir / "trades.csv", index=False)
    accounts.to_csv(day_dir / "accounts.csv", index=False)
    # marker file for FileSensor
    (day_dir / "_SUCCESS").write_text("ok\n", encoding="utf-8")
    print(f"wrote {trade_date} bad={bad} trades={len(trades)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(ROOT))
    args = parser.parse_args()
    root = Path(args.root)
    raw = root / CFG["paths"]["raw"]
    raw.mkdir(parents=True, exist_ok=True)
    bad_date = CFG["bad_batch_date"]
    for d in CFG["seed_dates"]:
        write_batch(d, raw, bad=(d == bad_date))


if __name__ == "__main__":
    main()

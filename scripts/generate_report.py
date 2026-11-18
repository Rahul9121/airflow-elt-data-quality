"""Build the static dark-theme HTML report from gate and alert artifacts."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CFG = yaml.safe_load((ROOT / "config" / "pipeline.yaml").read_text(encoding="utf-8"))


def _load_json_dir(path: Path) -> list[dict]:
    if not path.exists():
        return []
    items = []
    for p in sorted(path.glob("*.json")):
        items.append(json.loads(p.read_text(encoding="utf-8")))
    return items


def _badge(ok: bool) -> str:
    return (
        '<span class="badge pass">PASS</span>'
        if ok
        else '<span class="badge fail">FAIL</span>'
    )


def build_html(results: list[dict], alerts: list[dict], quarantine: list[dict]) -> str:
    rows = []
    for r in results:
        checks_html = "".join(
            f"<tr><td>{c['name']}</td><td>{_badge(c['passed'])}</td>"
            f"<td>{c.get('metric')}</td><td>{c.get('threshold')}</td>"
            f"<td class='mono'>{c.get('detail')}</td></tr>"
            for c in r.get("checks", [])
        )
        rows.append(
            f"""
            <section class="card">
              <h3>{r.get('trade_date')} {_badge(bool(r.get('passed')))}</h3>
              <p>raw rows: <b>{r.get('raw_trade_count')}</b> · processed: <b>{r.get('processed_trade_count')}</b></p>
              <table>
                <thead><tr><th>Check</th><th>Result</th><th>Metric</th><th>Threshold</th><th>Detail</th></tr></thead>
                <tbody>{checks_html}</tbody>
              </table>
            </section>
            """
        )

    alert_rows = "".join(
        f"<tr><td>{a.get('trade_date')}</td><td class='mono'>{a.get('message')}</td>"
        f"<td>{a.get('ts')}</td></tr>"
        for a in alerts
    ) or "<tr><td colspan='3'>No alerts</td></tr>"

    q_rows = "".join(
        f"<tr><td>{q.get('trade_date')}</td><td>{q.get('reason')}</td>"
        f"<td class='mono'>{q.get('path')}</td></tr>"
        for q in quarantine
    ) or "<tr><td colspan='3'>No quarantine records</td></tr>"

    blocked = [r for r in results if not r.get("passed")]
    blocked_txt = ", ".join(r["trade_date"] for r in blocked) or "none"

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Airflow ELT · Data Quality Gates</title>
<style>
:root {{
  --bg:#05060f; --cyan:#00f0ff; --mag:#ff2bd6; --lime:#b6ff00; --text:#e8eefc; --muted:#8b93a7; --card:#0c0f1c;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; font-family: ui-sans-serif, system-ui, Segoe UI, sans-serif; background:var(--bg); color:var(--text); }}
header {{ padding:2.5rem 1.5rem 1rem; border-bottom:1px solid #1a2035; }}
h1 {{ margin:0; font-size:1.8rem; letter-spacing:.04em; }}
h1 span {{ color:var(--cyan); }}
.sub {{ color:var(--muted); margin-top:.4rem; }}
main {{ max-width:1100px; margin:0 auto; padding:1.5rem; }}
.grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:1rem; margin:1rem 0 2rem; }}
.stat {{ background:var(--card); border:1px solid #1c243c; border-radius:12px; padding:1rem; }}
.stat b {{ display:block; font-size:1.4rem; color:var(--lime); margin-top:.3rem; }}
.card {{ background:var(--card); border:1px solid #1c243c; border-radius:14px; padding:1.2rem; margin-bottom:1.2rem; }}
h2 {{ color:var(--cyan); font-size:1.1rem; margin-top:2rem; }}
h3 {{ margin-top:0; }}
table {{ width:100%; border-collapse:collapse; font-size:.9rem; }}
th, td {{ text-align:left; padding:.55rem .4rem; border-bottom:1px solid #1a2035; vertical-align:top; }}
th {{ color:var(--muted); font-weight:600; }}
.badge {{ display:inline-block; padding:.15rem .55rem; border-radius:999px; font-size:.75rem; font-weight:700; letter-spacing:.04em; }}
.badge.pass {{ background:rgba(182,255,0,.12); color:var(--lime); border:1px solid rgba(182,255,0,.35); }}
.badge.fail {{ background:rgba(255,43,214,.12); color:var(--mag); border:1px solid rgba(255,43,214,.4); }}
.mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size:.82rem; color:#c9d4f0; }}
.mermaid-wrap {{ background:#080b16; border:1px solid #1c243c; border-radius:14px; padding:1rem; overflow:auto; }}
footer {{ color:var(--muted); text-align:center; padding:2rem 1rem 3rem; font-size:.85rem; }}
a {{ color:var(--cyan); }}
</style>
</head>
<body>
<header>
  <h1>Airflow ELT <span>Data Quality Gates</span></h1>
  <p class="sub">Trade-data ELT with PySpark, dbt/DuckDB, and hard quality gates · generated {now}</p>
</header>
<main>
  <div class="grid">
    <div class="stat">Runs reviewed<b>{len(results)}</b></div>
    <div class="stat">Passed<b>{sum(1 for r in results if r.get('passed'))}</b></div>
    <div class="stat">Blocked batches<b>{len(blocked)}</b></div>
    <div class="stat">Alerts recorded<b>{len(alerts)}</b></div>
  </div>

  <h2>DAG flow</h2>
  <div class="mermaid-wrap">
    <pre class="mermaid">
flowchart LR
  A[FileSensor raw batch] --> B[Ingest]
  B --> C[PySpark transform]
  C --> D[dbt models DuckDB]
  D --> E[Quality gates]
  E -->|pass| F[Publish HTML report]
  E -->|fail| G[Quarantine + alert]
  G --> H[Branch blocks publish]
    </pre>
  </div>

  <h2>Blocked bad batch</h2>
  <div class="card">
    <p>Configured bad batch date: <b class="mono">{CFG['bad_batch_date']}</b></p>
    <p>Blocked in this report: <b class="mono">{blocked_txt}</b></p>
    <p>The bad batch injects schema drift, null spikes, out-of-range values, invalid sides, and orphan account keys so the gate fails loudly.</p>
  </div>

  <h2>Per-run check results</h2>
  {''.join(rows) if rows else '<div class="card">No gate results found. Run the pipeline first.</div>'}

  <h2>Quarantine record</h2>
  <div class="card">
    <table>
      <thead><tr><th>Date</th><th>Reason</th><th>Path</th></tr></thead>
      <tbody>{q_rows}</tbody>
    </table>
  </div>

  <h2>Alerts</h2>
  <div class="card">
    <table>
      <thead><tr><th>Date</th><th>Message</th><th>Timestamp</th></tr></thead>
      <tbody>{alert_rows}</tbody>
    </table>
  </div>
</main>
<footer>
  Rahul Ega · <a href="https://github.com/Rahul9121/airflow-elt-data-quality">github.com/Rahul9121/airflow-elt-data-quality</a>
</footer>
<script type="module">
  import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
  mermaid.initialize({{ startOnLoad: true, theme: "dark", securityLevel: "loose" }});
</script>
</body>
</html>
"""


def main() -> None:
    results_dir = ROOT / "data" / "processed" / "gate_results"
    alerts_dir = ROOT / CFG["paths"]["alerts"]
    q_dir = ROOT / CFG["paths"]["quarantine"]
    results = _load_json_dir(results_dir)
    alerts = _load_json_dir(Path(alerts_dir))
    quarantine = _load_json_dir(Path(q_dir))
    # also accept quarantine meta json next to copied files
    out = ROOT / CFG["paths"]["report"]
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(build_html(results, alerts, quarantine), encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()

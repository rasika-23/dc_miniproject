"""CSV reports for experiment results."""
from __future__ import annotations

import csv
import io
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from metrics.calculator import summarize
from storage.event_repository import EventRepository


def _runs_table(rows: List[dict]) -> List[dict]:
    """Pivot metric_results rows (long) into one wide row per experiment run."""
    table: Dict[tuple, dict] = {}
    for r in rows:
        key = (r["experiment_id"], r["run_index"])
        entry = table.setdefault(key, {
            "experiment_id": r["experiment_id"], "scenario": r["scenario"], "run": r["run_index"] + 1,
            "recorded_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["ts"]))})
        entry[f"{r['metric_name']} ({r['unit']})" if r["unit"] else r["metric_name"]] = r["value"]
    return sorted(table.values(), key=lambda x: (x["recorded_at"], x["run"]))


def _summary_table(rows: List[dict]) -> List[dict]:
    groups: Dict[tuple, List[Optional[float]]] = {}
    for r in rows:
        groups.setdefault((r["scenario"], r["metric_name"], r["unit"]), []).append(r["value"])
    out = []
    for (scenario, metric, unit), values in sorted(groups.items()):
        s = summarize(values)
        out.append({"scenario": scenario, "metric": metric, "unit": unit, "runs": s["n"],
                    "average": None if s["mean"] is None else round(s["mean"], 3),
                    "min": s["min"], "max": s["max"]})
    return out


def _to_csv(rows: List[dict]) -> str:
    if not rows:
        return ""
    cols: List[str] = []
    for r in rows:
        for k in r:
            if k not in cols:
                cols.append(k)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def runs_csv(metric_rows: List[dict]) -> str:
    """One row per experiment run, one column per metric."""
    return _to_csv(_runs_table(metric_rows))


def summary_csv(metric_rows: List[dict]) -> str:
    """Average / min / max per scenario and metric across repeated runs."""
    return _to_csv(_summary_table(metric_rows))


def export_experiment(repo: EventRepository, out_dir: Path | str, experiment_id: Optional[str] = None) -> List[Path]:
    """Write <id>_runs.csv and <id>_summary.csv. With no id, exports everything stored."""
    rows = repo.metric_results(experiment_id=experiment_id)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = experiment_id or f"all_experiments_{time.strftime('%Y%m%d_%H%M%S')}"
    paths = []
    for suffix, text in (("runs", runs_csv(rows)), ("summary", summary_csv(rows))):
        p = out / f"{stem}_{suffix}.csv"
        p.write_text(text, encoding="utf-8", newline="")
        paths.append(p)
    return paths

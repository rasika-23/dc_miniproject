"""Run one or more experiment scenarios against the running cluster and print a summary.

    python scripts/run_experiment.py --list
    python scripts/run_experiment.py coordinator_crash --reps 3 --csv results/
    python scripts/run_experiment.py delayed_heartbeat --param delay_ms=4000 --param duration_s=10
    python scripts/run_experiment.py all
"""
import _bootstrap  # noqa: F401

import argparse
from pathlib import Path

from fault_injection.controller import ClusterController
from metrics.calculator import summarize
from metrics.experiment_runner import SCENARIOS, run_scenario
from metrics.export_results import export_experiment
from node.config import load_config


def _parse(value: str):
    for cast in (int, float):
        try:
            return cast(value)
        except ValueError:
            pass
    return {"true": True, "false": False}.get(value.lower(), value)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario", nargs="?", help="scenario key, or 'all'")
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--param", action="append", default=[], help="name=value (repeatable)")
    ap.add_argument("--csv", default=None, help="directory for CSV reports")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--config", default=None)
    args = ap.parse_args()

    if args.list or not args.scenario:
        for s in SCENARIOS.values():
            print(f"{s.key:22s} {s.title}\n{'':22s} {s.description}")
            for k, (d, h) in s.params.items():
                print(f"{'':22s}   --param {k}={d}   ({h})")
        return

    keys = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    params = {k: _parse(v) for k, v in (p.split("=", 1) for p in args.param)}
    ctrl = ClusterController(load_config(args.config))
    for key in keys:
        print(f"\n=== {SCENARIOS[key].title} ({args.reps} run(s)) ===")
        results = run_scenario(ctrl, key, args.reps, params if args.scenario != "all" else {},
                               progress=lambda msg, frac: print(f"  [{frac * 100:3.0f}%] {msg}"))
        for r in results:
            status = "PASS" if r.passed else "FAIL"
            print(f"  run {r.run_index + 1}: {status}" + (f"  error: {r.error}" if r.error else ""))
            for name, ok in r.checks.items():
                print(f"      [{'x' if ok else ' '}] {name}")
        names = sorted({n for r in results for n in r.metrics})
        for n in names:
            if n == "checks_passed_pct":
                continue
            s = summarize(r.metrics[n]["value"] for r in results if n in r.metrics)
            unit = next(r.metrics[n]["unit"] for r in results if n in r.metrics)
            if s["mean"] is not None:
                print(f"  {n:32s} avg {s['mean']:>10.1f}  min {s['min']:>10.1f}  max {s['max']:>10.1f} {unit}")
        if args.csv:
            for p in export_experiment(ctrl.repo, Path(args.csv), results[0].experiment_id):
                print(f"  wrote {p}")


if __name__ == "__main__":
    main()

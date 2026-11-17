#!/usr/bin/env python3
"""Turns the Arrow zero-copy sweep into Markdown tables (ns per input record).

    ./benchmark/arrow-zero-copy/report.py [results-dir] > benchmark/arrow-zero-copy/results/sweep-report.md

Reads sweep.csv (run.sh sweep) from results-dir (default results/ next to this script).
"""

import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"


def table(header: list[str], rows: list[list[str]]) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(out)


def main() -> None:
    runs = defaultdict(list)
    for row in csv.DictReader((RESULTS / "sweep.csv").open()):
        if row["net_ms"]:
            ns = int(row["net_ms"]) * 1e6 / int(row["count"])
            runs[(row["variant"], int(row["k"]), row["batch"])].append(ns)
    med = {key: statistics.median(v) for key, v in runs.items()}
    spread = {key: (max(v) - min(v)) / statistics.median(v) for key, v in runs.items() if len(v) > 1}
    ks = sorted({k for _, k, _ in med})
    batches = sorted({int(b) for v, _, b in med if v == "arrow"})
    n_runs = max(len(v) for v in runs.values())

    def get(variant: str, k: int, b: int | None = None) -> float | None:
        return med.get((variant, k, "" if b is None else str(b)))

    def fmt(x: float | None) -> str:
        return "—" if x is None else f"{x:.0f}"

    def ratio(fast: float | None, slow: float | None) -> str:
        return "—" if fast is None or slow is None else f"{slow / fast:.2f}×"

    print("## Arrow zero-copy vs copying native operator (local MiniCluster, whole job)\n")
    print(f"ns per record, median of {n_runs} runs. Includes the data generation.\n")
    header = ["k", "tuned Java"] + [f"native B={b}" for b in batches] + [f"arrow B={b}" for b in batches]
    rows = [[str(k), fmt(get("tuned", k))]
            + [fmt(get("native", k, b)) for b in batches]
            + [fmt(get("arrow", k, b)) for b in batches] for k in ks]
    print(table(header, rows))

    print("\n**Arrow speed-up over the copying native operator at the same batch size** (>1 = Arrow faster):\n")
    rows = [[str(k)] + [ratio(get("arrow", k, b), get("native", k, b)) for b in batches] for k in ks]
    print(table(["k"] + [f"B={b}" for b in batches], rows))

    print("\n**Speed-up over tuned Java** (>1 = faster than tuned Java):\n")
    rows = [[str(k)]
            + [ratio(get("native", k, b), get("tuned", k)) for b in batches]
            + [ratio(get("arrow", k, b), get("tuned", k)) for b in batches] for k in ks]
    print(table(["k"] + [f"native B={b}" for b in batches] + [f"arrow B={b}" for b in batches], rows))

    if spread:
        typical = statistics.median(spread.values())
        worst_key = max(spread, key=spread.get)
        print(f"\nRun-to-run spread (max-min over median): typical {typical:.1%}, worst"
              f" {spread[worst_key]:.1%} ({worst_key[0]}, k={worst_key[1]}"
              f"{', B=' + worst_key[2] if worst_key[2] else ''}).")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        RESULTS = Path(sys.argv[1])
    main()

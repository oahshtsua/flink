#!/usr/bin/env python3
"""Turns the Arrow zero-copy sweep into Markdown tables (ns per input record).

    ./benchmark/arrow-zero-copy/report.py [results-dir] > benchmark/arrow-zero-copy/results/sweep-report.md

Reads sweep.csv (run.sh sweep) from results-dir (default results/ next to this script). Variants
missing from the file (e.g. the Java engines in the step 1 sweep) are left out.
"""

import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"

# variant in sweep.csv -> label, in table order
VARIANTS = {
    "native": "copying native (idea 4)",
    "arrow": "Arrow + C",
    "arrow-java": "Arrow + Java, one function",
    "arrow-steps": "Arrow + Java, four steps",
}


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
    batches = sorted({int(b) for _, _, b in med if b})
    variants = [v for v in VARIANTS if any(key[0] == v for key in med)]
    n_runs = max(len(v) for v in runs.values())

    def get(variant: str, k: int, b: int | None = None) -> float | None:
        return med.get((variant, k, "" if b is None else str(b)))

    def fmt(x: float | None) -> str:
        return "—" if x is None else f"{x:.0f}"

    def ratio(fast: float | None, slow: float | None) -> str:
        return "—" if fast is None or slow is None else f"{slow / fast:.2f}×"

    k_header = [f"k={k}" for k in ks]
    print("## Inside Flink (local MiniCluster, whole job)\n")
    print(f"ns per record, median of {n_runs} runs. Includes the data generation. B = readings per"
          " batch.\n")
    rows = [["tuned Java (one record at a time)", ""] + [fmt(get("tuned", k)) for k in ks]]
    for v in variants:
        rows += [[VARIANTS[v], str(b)] + [fmt(get(v, k, b)) for k in ks] for b in batches]
    print(table(["variant", "B"] + k_header, rows))

    print("\n**Speed-up over tuned Java** (>1 = faster than tuned Java):\n")
    rows = []
    for v in variants:
        rows += [[VARIANTS[v], str(b)] + [ratio(get(v, k, b), get("tuned", k)) for k in ks]
                 for b in batches]
    print(table(["variant", "B"] + k_header, rows))

    if "arrow" in variants:
        print("\n**Against Arrow + C at the same B** (>1 = faster than Arrow + C):\n")
        rows = []
        for v in variants:
            if v == "arrow":
                continue
            rows += [[VARIANTS[v], str(b)] + [ratio(get(v, k, b), get("arrow", k, b)) for k in ks]
                     for b in batches]
        print(table(["variant", "B"] + k_header, rows))

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

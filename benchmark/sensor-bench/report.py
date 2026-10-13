#!/usr/bin/env python3
"""Turns the batch-size x k sweeps into Markdown tables (ns per input record).

    ./benchmark/sensor-bench/report.py > benchmark/sensor-bench/results/sweep-report.md

Reads, from results/ (whichever exist):
  sweep-java.json         stage 1, Java  (run.sh 'SensorBatch.(generate|java)$' -p batch=1024)
  sweep-native.json       stage 1, C     (run.sh 'SensorBatch.nativePath$')
  minicluster-sweep.csv   stage 2        (minicluster-sweep.sh)
"""

import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"


def fmt(x: float | None) -> str:
    return "—" if x is None else f"{x:.0f}"


def ratio(native: float | None, base: float | None) -> str:
    if native is None or base is None:
        return "—"
    return f"{base / native:.2f}×"


def table(header: list[str], rows: list[list[str]]) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(out)


def inside_flink() -> None:
    path = RESULTS / "minicluster-sweep.csv"
    if not path.exists():
        return
    runs = defaultdict(list)
    for row in csv.DictReader(path.open()):
        if row["net_ms"]:
            ns = int(row["net_ms"]) * 1e6 / int(row["count"])
            runs[(row["variant"], int(row["k"]), row["batch"])].append(ns)
    med = {key: statistics.median(v) for key, v in runs.items()}
    spread = {key: (max(v) - min(v)) / statistics.median(v) for key, v in runs.items() if len(v) > 1}
    ks = sorted({k for _, k, _ in med})
    batches = sorted({int(b) for v, _, b in med if v == "native"})
    n_runs = max(len(v) for v in runs.values())

    print("## Stage 2 — plugged into Flink (local MiniCluster, whole job)\n")
    print(f"ns per record, median of {n_runs} runs. Includes ~40 ns/record of data generation.\n")
    header = ["k", "stock", "fusion"] + [f"native B={b}" for b in batches]
    rows = []
    for k in ks:
        rows.append(
            [str(k), fmt(med.get(("stock", k, ""))), fmt(med.get(("fusion", k, "")))]
            + [fmt(med.get(("native", k, str(b)))) for b in batches]
        )
    print(table(header, rows))

    print("\n**Native speed-up over the fusion prototype** (>1 = native faster):\n")
    header = ["k"] + [f"B={b}" for b in batches]
    rows = []
    for k in ks:
        base = med.get(("fusion", k, ""))
        rows.append([str(k)] + [ratio(med.get(("native", k, str(b))), base) for b in batches])
    print(table(header, rows))

    if spread:
        worst = max(spread.items(), key=lambda kv: kv[1])
        print(
            f"\nRun-to-run spread (max-min over median): typical "
            f"{statistics.median(spread.values()) * 100:.1f}%, worst "
            f"{worst[1] * 100:.1f}% ({worst[0][0]}, k={worst[0][1]}"
            f"{', B=' + worst[0][2] if worst[0][2] else ''}).\n"
        )


def load_jmh(name: str) -> dict:
    path = RESULTS / name
    if not path.exists():
        return {}
    out = {}
    for r in json.loads(path.read_text()):
        bench = r["benchmark"].rsplit(".", 1)[1]
        p = r.get("params", {})
        out[(bench, int(p["k"]), int(p["batch"]))] = (
            r["primaryMetric"]["score"],
            r["primaryMetric"]["scoreError"],
        )
    return out


def outside_flink() -> None:
    java = load_jmh("sweep-java.json")
    native = load_jmh("sweep-native.json")
    if not java and not native:
        return
    ks = sorted({k for _, k, _ in list(java) + list(native)})
    batches = sorted({b for bench, _, b in native if bench == "nativePath"})

    def score(d: dict, bench: str, k: int, b: int | None = None) -> float | None:
        if b is None:
            hits = [v[0] for (bb, kk, _), v in d.items() if bb == bench and kk == k]
            return hits[0] if hits else None
        v = d.get((bench, k, b))
        return v[0] if v else None

    print("## Stage 1 — Java vs C, on their own (JMH; C includes copying + call)\n")
    print("ns per record, including data generation (column `generate`).\n")
    header = ["k", "generate", "Java"] + [f"C, B={b}" for b in batches]
    rows = []
    for k in ks:
        rows.append(
            [str(k), fmt(score(java, "generate", k)), fmt(score(java, "java", k))]
            + [fmt(score(native, "nativePath", k, b)) for b in batches]
        )
    print(table(header, rows))

    print("\n**C speed-up over Java** (>1 = C faster):\n")
    header = ["k"] + [f"B={b}" for b in batches]
    rows = []
    for k in ks:
        base = score(java, "java", k)
        rows.append([str(k)] + [ratio(score(native, "nativePath", k, b), base) for b in batches])
    print(table(header, rows))
    print()


def main() -> int:
    outside_flink()
    inside_flink()
    return 0


if __name__ == "__main__":
    sys.exit(main())

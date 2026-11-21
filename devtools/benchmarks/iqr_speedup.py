#!/usr/bin/env python3
"""Per-block speedup distribution for `ckb replay --verify-only --timing-csv`.

Collect per-block timings first (the node must be stopped, or use a DB copy):

    mkdir -p /tmp/ckb-iqr
    for n in 1 2 4 8 16 32; do
        CKB_CONTEXTUAL_VERIFY_THREADS=$n ./target/prod/ckb -C <node-data> \\
            replay --verify-only --timing-csv /tmp/ckb-iqr/n$n.csv <from> <to>
    done

Then compute the table:

    python3 devtools/benchmarks/iqr_speedup.py --input-dir /tmp/ckb-iqr

The 1-thread CSV is the baseline. The speedup of block i at n threads is
`t_i(1) / t_i(n)`, using the `txs_verify_ns` column: the parallel
per-transaction phase only. Transaction resolution and CSV writing are not
part of that measurement.

By default only blocks with at least `--min-txs` transactions are included,
since blocks with very few transactions cannot use many threads and add
low-ratio outliers. Use `--min-txs 1` for all non-empty blocks.
"""

import argparse
import csv
import sys
from pathlib import Path


def load_timings(path):
    """Return {block_number: (tx_count, txs_verify_ns)}."""
    with open(path, newline="") as file:
        return {
            int(row["block"]): (int(row["tx_count"]), int(row["txs_verify_ns"]))
            for row in csv.DictReader(file)
        }


def percentile(sorted_values, quantile):
    """Linear interpolation percentile, matching numpy's default."""
    if not sorted_values:
        return float("nan")
    position = quantile * (len(sorted_values) - 1)
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return sorted_values[lower] * (1 - fraction) + sorted_values[upper] * fraction


def compute_row(baseline, sample, min_txs):
    blocks = [
        block
        for block in baseline
        if block in sample
        and baseline[block][0] >= min_txs
        and baseline[block][1] > 0
        and sample[block][1] > 0
    ]
    if not blocks:
        return None

    ratios = sorted(baseline[block][1] / sample[block][1] for block in blocks)
    aggregate = sum(baseline[block][1] for block in blocks) / sum(
        sample[block][1] for block in blocks
    )
    mean = sum(ratios) / len(ratios)
    p25, median, p75, p95 = (
        percentile(ratios, quantile) for quantile in (0.25, 0.50, 0.75, 0.95)
    )
    return {
        "blocks": len(blocks),
        "aggregate": aggregate,
        "median": median,
        "p25": p25,
        "p75": p75,
        "p95": p95,
        "iqr": p75 - p25,
        "mean": mean,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Compute per-block speedup median/IQR from --timing-csv files."
    )
    parser.add_argument(
        "--input-dir",
        default="/tmp/ckb-iqr",
        help="Directory containing n<N>.csv files (default: /tmp/ckb-iqr)",
    )
    parser.add_argument(
        "--baseline",
        default=None,
        help="Baseline CSV, usually the 1-thread run (default: <input-dir>/n1.csv)",
    )
    parser.add_argument(
        "--threads",
        default="2,4,8,16,32",
        help="Comma-separated thread counts to report (default: 2,4,8,16,32)",
    )
    parser.add_argument(
        "--min-txs",
        type=int,
        default=100,
        help="Only include blocks with at least this many transactions (default: 100)",
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "csv"),
        default="markdown",
        help="Output format (default: markdown)",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    baseline_path = Path(args.baseline) if args.baseline else input_dir / "n1.csv"
    if not baseline_path.is_file():
        sys.exit(f"baseline CSV not found: {baseline_path}")

    threads = [int(value) for value in args.threads.split(",") if value.strip()]
    baseline = load_timings(baseline_path)

    rows = []
    for threads_count in threads:
        path = input_dir / f"n{threads_count}.csv"
        if not path.is_file():
            print(f"warning: {path} not found, skipping", file=sys.stderr)
            continue
        row = compute_row(baseline, load_timings(path), args.min_txs)
        if row is None:
            print(f"warning: no blocks matched for {threads_count} threads", file=sys.stderr)
            continue
        rows.append((threads_count, row))

    if args.format == "csv":
        print("threads,blocks,aggregate,median,p25,p75,p95,iqr,mean")
        for threads_count, row in rows:
            print(
                f"{threads_count},{row['blocks']},{row['aggregate']:.4f},"
                f"{row['median']:.4f},{row['p25']:.4f},{row['p75']:.4f},"
                f"{row['p95']:.4f},{row['iqr']:.4f},{row['mean']:.4f}"
            )
        return

    print(
        "| threads | Aggregate speedup | Median (p50) | p25 | p75 | p95 "
        "| IQR (p75−p25) | Mean |"
    )
    print("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for threads_count, row in rows:
        print(
            f"| {threads_count} | {row['aggregate']:.2f}× "
            f"| {row['median']:.2f}× | {row['p25']:.2f}× | {row['p75']:.2f}× "
            f"| {row['p95']:.2f}× | {row['iqr']:.2f} | {row['mean']:.2f}× |"
        )


if __name__ == "__main__":
    main()

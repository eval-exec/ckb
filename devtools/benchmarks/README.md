# verify-only benchmark helpers

Tools for measuring contextual block verification with the dedicated rayon
pool introduced by `CKB_CONTEXTUAL_VERIFY_THREADS` and the read-only
`ckb replay --verify-only` mode.

## 1. Build

```bash
make prod
./target/prod/ckb replay --help | grep timing-csv   # flag must be present
```

The node must be stopped while the benchmark opens the chain DB, or point
`-C` at a copy of the data directory.

## 2. Collect per-block timings

`n1` (one thread) is required: it is the baseline for the per-block speedup.

```bash
mkdir -p /tmp/ckb-iqr
for n in 1 2 4 8 16 32; do
    CKB_CONTEXTUAL_VERIFY_THREADS=$n ./target/prod/ckb -C <node-data> \
        replay --verify-only --timing-csv /tmp/ckb-iqr/n$n.csv <from> <to>
done
```

Use a contiguous range of fully populated blocks (for example
`11990222 11991221`, 1,000 blocks / 491,026 txs on mainnet). Do not run
configurations concurrently — they compete for CPU and invalidate the numbers.

## 3. Compute the distribution

```bash
python3 devtools/benchmarks/iqr_speedup.py --input-dir /tmp/ckb-iqr
```

This prints the aggregate speedup plus the per-block distribution
(median, p25, p75, p95, IQR) measured from `txs_verify_ns`, the parallel
per-transaction phase. Use `--min-txs 1` for all non-empty blocks,
`--format csv` for the paper, or `--threads 2,4,8,16,32` to choose rows.

## Notes

- `txs_verify_ns` excludes transaction resolution and CSV writing; those are
  reported separately by the harness.
- On hybrid CPUs (Intel P/E cores), throughput can peak at the number of
  P-cores and degrade above it; cap `CKB_CONTEXTUAL_VERIFY_THREADS`
  accordingly. On homogeneous server CPUs, scaling continues to the physical
  core count and SMT adds little.

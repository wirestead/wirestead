# Performance Validation

Performance validation for releases should preserve benchmark artifacts that can
be compared across versions and environments.

## Required measurements

- TCP loopback latency: p50, p95, p99
- UDS loopback latency: p50, p95, p99
- UDP latency, loss, and payload-size behavior
- Throughput by payload size: 64 B, 1 KiB, 16 KiB, 64 KiB, 1 MiB
- `send` vs `try_send` vs `send_move` vs `send_shared`
- `Reliable` vs `BestEffort`
- slow-consumer fan-out behavior
- queue growth, `pending_bytes`, and `dropped_bytes`
- callback throughput

## Initial soft gates

Nightly and manual benchmark runs should start as warning-only checks:

- p99 latency regression greater than 30%
- throughput regression greater than 20%
- unexpected allocation-count increase
- unexpected growth in `queued_bytes` or `pending_bytes`

Hard gates should be limited to stable, low-noise checks until v1.0 release
candidates establish a reliable baseline.

## Artifacts

Benchmark runs should preserve:

- `benchmark-result.json`
- `benchmark-summary.md`
- environment metadata: OS, kernel, compiler, Boost version, CPU model, governor,
  and container/VM status when applicable

Release notes and `docs/release_checklist.md` should link the latest relevant
benchmark artifact or the corresponding Wirestead benchmark result.

`scripts/run_benchmarks.sh` is the core repository's manual/nightly harness. It
consumes an installed Wirestead package via `--cmake-prefix`, builds a small
external benchmark consumer, and records TCP loopback latency, payload-size
throughput, send variant acceptance, and queue snapshot metrics. The benchmark
workflow runs this harness as a soft gate; compare the generated JSON against
the current release baseline before promoting any metric to a hard gate.

# Follow-up completion evidence, 2026-10-17

## ARM64 investigation and patch judgment

The target was the Windows ARM64 TCP concurrent-stop heap corruption from
#713. It was reproduced twice with exit `0xc0000374`: plain execution 360
(359 passed first), then capture-enabled execution 88 (87 passed first).
Another 500 capture-enabled executions passed. No dump was collected, so the
allocation/free fault and root cause remain unresolved. A patch claiming to
fix that heap corruption is not ready. No speculative runtime change was made.

[Investigation, environments, artifacts and residual risk](arm64_heap_investigation_20261017.md)
records both successful and failed attempts and the dump-collection gap.
The next investigation step is to validate direct debugger/WER collection;
more successful repetitions alone cannot close this defect.

## #714 baseline validation

[#714](https://github.com/wirestead/wirestead/pull/714) changes only
`VCPKG_BASELINE` from `19780d9cdf84d0944cf9a318666703b89ab6629c` to
`e456309491fd875487bf02b8a0dca76c1b1b00cc`.
The PR head remained `c896a9c30c422dee97d62006741a8f8126eb4803` throughout
validation. All three originally failed workflows were rerun in full on that
same head, not just the failed jobs:

| Workflow | Attempt 1 | Attempt 2 |
| --- | --- | --- |
| [Code Coverage](https://github.com/wirestead/wirestead/actions/runs/38261756295/attempts/2) | Failed in vcpkg bootstrap download (HTTP 504) | Success |
| [CMake](https://github.com/wirestead/wirestead/actions/runs/38261756110/attempts/2) | Linux ARM64 dependency bootstrap failed (HTTP 504) | Success on all five platform jobs |
| [CI](https://github.com/wirestead/wirestead/actions/runs/38261756296/attempts/2) | macOS dependency bootstrap failed (HTTP 504), downstream tests skipped | Success |

The previously skipped unit test matrix (Linux x64/ARM64 and macOS), TLS,
Linux x64/ARM64 integration, Memory Safety Tests and E2E Tests all passed.
Windows x64 and Windows ARM64 Release/smoke jobs passed too. Coverage's release
notification remained skipped because this is a pull request, not a release;
it is not a missing test. The Dependabot auto-merge job is also not a required
test for this non-Dependabot PR.

[Exact attempt-2 job outcomes and job links](evidence/20261017/baseline-714-results.json)
are preserved. The candidate's CI verification gap is closed. The baseline
change can now be assessed for merge with those results; no baseline was
changed or merged as part of this investigation. The Windows ARM heap risk
above remains separate even though this single CI smoke run passed.

## #710 policy, implementation and HIL evidence

The agreed direction is existing defaults plus an explicit DTR retention
option. [PR #715](https://github.com/wirestead/wirestead/pull/715) implements
`keep_dtr_on_close()` with default `false`, retaining existing DTR/HUPCL,
reconnect and watchdog defaults. It documents persistence of the HUPCL change,
first-open/unplug limitations and unsupported-driver failures.

Implementation commit: `6546bbddb5a739233e0c2d5bdb70b0e63b671e97`.
Native Orin/UNO Mini HIL: baseline 15/15, candidate 20/20; focused related tests
146/146. All five explicit-retention executions preserved EEPROM boot counts
through stop/start and watchdog reopen. The PR contains the exact board,
kernel, toolchain, repetition counts, raw logs and hashes in
[`docs/serial_dtr_retention_validation.md`](serial_dtr_retention_validation.md).

These are evidence and policy records, not a release or merge authorization.

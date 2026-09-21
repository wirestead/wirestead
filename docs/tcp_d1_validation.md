# TCP D-1 follow-up validation

## Scope and reference

This note records work after PR #652 head `757bd87e3866e654dcec26fac5c72ccef6b61b64`.
The historical audit remains a comparison against its original baseline.
This follow-up does not reuse earlier workers' pass counts or timing probes.

The changes keep request-only stop on the target executor and make every
outside caller wait for actual cleanup. Client completion also waits for
cancelled I/O handlers to leave buffers and internal post-processing. Server
completion includes each live session's strand cleanup. Wrapper state is
released only by a waiting caller after its callback gate is idle. Server
accept/retry/close work and client posted sends carry the run generation.

## Deterministic checks

- Saved injected-channel handlers are paused between liveness and admission.
  Stop completes, restart opens a new generation, the old handler is refused,
  and the new handler is delivered. Both wrappers are covered.
- Cleanup is paused immediately before its completion signal. Two outside
  callers remain blocked until it is released. Both TCP transports are covered.
- The client's final cancelled I/O handler is held before its internal
  post-processing exits. Both outside callers wait.
- A shared context is occupied with an unrelated handler ready. The stopping
  thread neither runs that handler nor returns before cleanup.
- Never-started transports stop without a running executor and leave no queued
  self-owning cleanup handler.
- Loopback cases hold callback bodies after their own stop returns, then check
  two outside callers and restart with actual reception. The shared-versus-
  independent executor case remains covered.

Scheduling hooks are internal and unset outside tests. Test release guards
keep signals, channels, and executors alive on failed assertion paths.

## Commands and results

Validation is performed inside Ubuntu 24.04 WSL with GCC, Debug, TLS disabled:

```sh
cmake --build build -j2
ctest --test-dir build --output-on-failure -j2
git diff --check
```

Final full-suite result: 841 discovered cases, 829 passed, 12 skipped,
0 failed (41.49 seconds). The skips are the existing UDP 4 KiB/16 KiB
large-payload cases; they are not reported as passed.

AddressSanitizer uses a separate build with BOTH
`WIRESTEAD_ENABLE_SANITIZERS=ON` and `WIRESTEAD_ENABLE_ASAN=ON`.
The library compile flags are checked for `-fsanitize=address`.
The three D-1 executables run with `ASAN_OPTIONS=detect_leaks=1`.

Final ASan result: 19/19 cases passed (9 admission/cleanup cases, 6 client
loopback cases, 4 server loopback cases), with no reported address or leak
sanitizer errors.

## Isolated comparison against the last reviewed head

A detached worktree at `../log/codex-d1-baseline-757bd87e3` has its own
build directory. It uses the identical admission-test source. Its library is
`757bd87e3` with only the internal scheduling-hook include/calls added
(three added lines in each transport); no fixed shutdown logic is copied.

After a successful build, the worktree's own absolute binary path is used:

```sh
build/bin/run_integration_test_tcp_stop_admission   --gtest_filter='*BothOutsideStopsWaitUntilCleanupActuallyCompletes*:*NeverStartedStopNeedsNoExecutorAndRetainsNoWork*'
```

All four selected cases fail there: both outside stops return while cleanup is
parked, and never-started stops retain queued cleanup references. The same four
cases pass in the fixed tree. This comparison is against `757bd87e3`, NOT
the pre-D1 tree, and is not a claim that all D-1 tests reproduce old defects.

## Test contract adjustments

Tests that manually drive external contexts in slices now supply executor
progress while calling the waiting stop. The old server
`ConcurrentStartStop` test produced a segmentation fault while overlapping
start with stop. D-1 explicitly excludes that overlap; it was replaced by
`RestartAfterConcurrentStops`, which waits for both stop callers before
starting the next run. This is an explicit test-contract change, not evidence
that overlapping start/stop has become safe.

## Limits

No UDS, UDP, or serial implementation changes; no D-2/D-3 implementation.
No guarantee of flush or peer delivery. No destruction from an object's own
callback. External executors must keep running; restart waits for all stop
callers. TLS-enabled syntax checks passed for the three changed transport translation
units. Local Windows/MSVC, macOS, TLS runtime, and ThreadSanitizer runs are
not part of this validation. The remote head's CI result must be reported separately
from these local measurements.


## Local evidence

Build/test logs and comparison metadata are retained outside the repository in
`../log/codex-d1-validation/`. The normal and ASan builds use distinct
directories, and the baseline executable comes from its detached worktree.

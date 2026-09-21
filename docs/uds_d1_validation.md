# UDS D-1 validation

This change follows TCP PR #652, merged as
`1dfc1cb16220698b0453139ae8d8f24322515d27`. It covers UDS client/server
transports and wrappers only.

## Contract and implementation

Every outside stop caller observes transport cleanup and an idle callback
gate. A caller running the target executor, including another channel sharing
that executor, requests shutdown and returns. An unrelated executor still
waits. External executors must continue progressing; stop does not poll them,
use elapsed-time fallbacks, or infer completion from a stopped context.

Client write submission is serialized with the cleanup post. Tracked I/O
completion ownership prevents a cancelled read/write/timer from outliving the
run's completion boundary. Active write buffers remain owned until completion.
The tracking adapter explicitly dispatches interface callbacks onto the strand,
and releases ownership when a test socket discards a completion handler.

The server serializes accept/retry/close forwarding with cleanup, isolates run
generations, and waits for every session's strand cleanup. Closed sessions may
retain their own cancelled I/O without affecting a later run. Session cleanup
is idempotent. Synchronous validation/bind failures and never-started servers
need no executor work to stop. Only a server that successfully bound its path
removes it during cleanup.

Wrapper admission, running callback count and closed state use one gate lock.
Each start opens a new generation, including injected channels. Outside stop
finalizes timers, framers and managed threads after callback postprocessing;
request-only stop retains the channel for a later outside completion call.

## Regression tests

- Admission tests park an old saved callback before admission, stop/restart
  the wrapper, then prove the old callback is refused and the new one runs.
- Transport tests park the actual cleanup signal while two outside stop
  callers wait, verify no unrelated executor pumping, and check never-started,
  failed-start and cancelled-I/O completion behavior.
- Loopback tests exercise concurrent outside stop, callback-initiated stop,
  connection callbacks, same versus unrelated executors, and restart followed
  by real data receipt.
- The server external-executor test also runs with two I/O threads so a free
  thread cannot finish shutdown while another thread is in a session callback.
- Existing UDS path ownership/security and session lifecycle tests remain.
  Manual-context tests now keep the executor progressing during outside stop.
  The deliberately stalled write fake is explicitly released before the final
  waiting stop.

## Before/after evidence

The existing isolated comparison worktree is at `757bd87e3`. Its UDS
transport/session/wrapper sources were byte-compared against merged main
`1dfc1cb16` and are identical; this is not a claim about its TCP code.
Only the two new UDS loopback test source files and their build registration
were added there. No UDS implementation fix or UDS stop instrumentation was
copied into the comparison library.

Both selected tests fail on that unchanged UDS implementation because stop
returns before the callback finishes:

- `UdsClientStopCompletionTest.OutsideStopWaitsOnAnExternallyRunContext`
- `UdsServerStopCompletionTest.OutsideStopWaitsWithTwoExecutorThreads`

The same cases pass with this implementation. Source equality hashes and
comparison logs are retained in the workspace's
`log/codex-uds-d1-validation` directory.

## Validation results

- GCC Debug, TLS off: full build with `cmake --build build -j2`;
  `ctest --test-dir build --output-on-failure -j2` discovers 863 tests:
  851 pass, 12 existing UDP large-payload tests skip, zero failures.
- AddressSanitizer + leak detection: both
  `WIRESTEAD_ENABLE_SANITIZERS=ON` and `WIRESTEAD_ENABLE_ASAN=ON`;
  executable linkage to `libasan.so.8` verified. With
  `ASAN_OPTIONS=detect_leaks=1:halt_on_error=1`, all 21 UDS D-1 tests
  and all 8 UDS server security/path-ownership tests pass, without sanitizer
  or leak reports.
- `git diff --check` and clang-format checks pass.
- Validation ran under Ubuntu 24.04 in WSL from the Windows desktop host.
  The new PR's remote CI is a separate validation stage.

## Limits

Restart is allowed only after all outstanding stop calls finish. Concurrent
start/stop and destruction from a callback are unsupported. Stop cancels; it
does not guarantee flushing queued bytes or peer delivery. External executors
must keep running while an outside caller waits. Windows UDS runtime, TSAN,
performance claims, UDP/serial and D-2/D-3 are not part of this validation.

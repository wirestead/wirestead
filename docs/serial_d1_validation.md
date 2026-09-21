# Serial D-1 validation

This follow-up starts at UDP PR #655, merged as
71e489364d0d6e3dccc79aefb941e60a42c7ae38 after 43 successful CI checks
(two non-applicable jobs skipped). It covers serial transport and wrapper D-1.

## Contract and implementation

Every outside stop caller waits for actual transport cleanup and admitted
wrapper callback completion. Calls on the target executor or inside the
target's callback request shutdown and return. An unrelated executor still
waits. External contexts must keep progressing: the library does not poll,
restart or stop them as a substitute for completion.

Serial port interfaces erase associated executors into std::function. Tracked
completion ownership explicitly dispatches read/write/retry/receive-idle work
onto the strand, and also accounts for injected ports discarding a handler.
Final cleanup is idempotent and waits for tracked I/O to release its references.
Gather-write buffers stay alive until completion or handler disposal. Owned
contexts finish naturally once cleanup releases the work guard; concurrent
outside callers serialize joining the owned thread.

A submission mutex orders accepted writes before the cleanup post. Before-start
writes reject without retaining work. Completed stop permits a fresh run.
Wrapper admission, callback count and generation switching share one gate;
delayed old data/state/backpressure callbacks and batch timers cannot enter
the next run. Outside finalization resets batches/framers and joins managed
threads after admitted callback postprocessing. Injected transports retain
their identity and get fresh handlers on restart.

Serial-specific queue overflow policy, retry configuration, RS-485/modem
options and blocking-send policy are preserved.

## Regression tests

The 25 new cases cover:

- Concurrent outside stops overlapping data callbacks and actual cleanup.
- Callback-initiated shutdown, outside completion and real PTY receipt after
  restart on the same injected channel.
- Owned and externally run contexts, with two external executor runners.
- Target-executor request-only stop versus an unrelated executor waiting.
- Connection and batch-timer callbacks requesting stop.
- Wrapper-managed contexts and the shared IoContextManager path, including
  callback stop, two outside callers, and receipt after restart.
- Saved data/state/backpressure callbacks parked before admission across
  stop/restart; old callbacks are refused and new callbacks still work.
- Never-started shutdown, before-start write rejection, no unrelated executor
  pumping, cancelled read completion and failed-open retry cancellation.
- A gather write retained after port close: buffers remain readable while
  outside stop waits. Explicit completion and handler disposal both release
  shutdown waiters.

Existing manual-context tests now continue executor progress during outside
stop. The intentionally stalled write fake is released before the final
waiting stop. Existing transport, callback-stop and configuration assertions
are retained.

## Before/after evidence

The comparison worktree is at 757bd87e3. Its serial transport and wrapper
sources were byte-compared against merged main 71e489364 and are identical;
this makes no claim about other transports in that worktree. Only the new
PTY test file and its build registration were added there.

Both external-context cases fail against that unchanged serial implementation
because outside stop returns before the held callback finishes:

- ConcurrentOutsideStopsWaitForCallback/1
- UnrelatedExecutorStillWaitsForCallback/1

The same cases pass here. Source hashes and comparison logs are retained in
the workspace's log/codex-serial-d1-validation directory.

## Validation results

- GCC Debug, TLS off: full build with cmake --build build -j2, then
  ctest --test-dir build --output-on-failure --timeout 60 -j2.
  918 discovered tests: 906 passed, 12 existing UDP large-payload tests
  skipped, zero failures.
- AddressSanitizer + leak detection: WIRESTEAD_ENABLE_SANITIZERS=ON and
  WIRESTEAD_ENABLE_ASAN=ON; libasan.so.8 linkage verified. With
  ASAN_OPTIONS=detect_leaks=1:halt_on_error=1, all 51 selected tests pass:
  25 new D-1 cases, 24 existing serial transport tests and the 2 earlier
  callback-stop regression tests. No sanitizer or leak reports were emitted.
- git diff --check and clang-format checks pass.
- Validation ran on Ubuntu 24.04 in WSL from the Windows desktop host.
  Remote CI is a separate validation stage.

## Limits

Restart follows completion of all outstanding stop calls. Concurrent start/stop,
restart inside a callback, and destruction before shutdown completes remain
unsupported. Stop cancels; it does not promise flushing or peer delivery.
PTY tests are POSIX-only; injected-port tests are portable. Actual UART,
USB serial, RS-485 hardware, Windows device runtime, TSAN and performance
claims are not validated here. D-2/D-3 remain separate work.

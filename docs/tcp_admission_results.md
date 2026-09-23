# TCP native admission results

Private TCP write helpers now return SendResult for copy, move and shared
buffers, including the try variants. The public Channel overrides still
return accepted(), and the wrapper's existing pinned calls do the same.
No public method signatures or Channel vtables change.

The helpers preserve the outcome at the point of decision:

- A never-started transport or completed explicit stop yields NotStarted.
- A requested explicit stop with cleanup still pending yields Stopping.
- An active run with an unavailable or replaced connection yields NotReady.
- Empty input or an empty/null shared buffer yields InvalidArgument when its
  validation branch is reached; input above MAX_BUFFER_SIZE yields TooLarge.
- Immediate capacity refusal yields WouldBlock.
- Successful reservation/submission yields an accepted result, not delivery.

Readiness and capacity outcomes are selected under the existing submission
mutex. Payload checks retain their existing placement. The result is a value:
later stop, loss or drain does not require inspecting current transport state
to reconstruct an earlier rejection.

## Boundary with the public contract

These are native admission-stage results, not the complete D-3 wrapper result.
The wrapper still needs to apply validation-first precedence (including the
reported whole-queue size limit), combine its own lifecycle and callback-gate state with the native lifecycle
result, and map capacity refusal according to call site and strategy.
BestEffort wrapper refusal must ultimately report QueueFull where specified.
Wait cancellation uses the connection record from PR #670 instead of these
immediate admission results.

Ordinary BestEffort native writes retain their existing deferred queue/drop
policy. Try writes retain their immediate refusal and existing dropped/failed
accounting. This change does not introduce new rejections, move payloads on
rejection, change retries, or alter post-acceptance accounting.

## Lifecycle observation

The native state check runs under the submission mutex used by stop requests
and connection admission. If stop was requested, it reads cleanup completion
under the stop condition-variable mutex. A Closed link state alone is not
proof that cancelled I/O and callbacks have completed. No executor polling,
timeout or follow-up state read is used to infer the result.

The native completion point describes transport cleanup. Wrapper callback-gate
and finalization state still need their own mapping when public APIs migrate.
Existing capacity wait records retain their first cause; an already selected
CancelledWhileWaiting or NotReady is not replaced by this immediate state check.

## Regression coverage

The original 24 parameterized cases cover all six write forms, both strategies
and enabled/disabled pooling. They check unavailable state, invalid/oversized
payloads, acceptance, deterministic capacity refusal with executor progress
paused, caller storage after rejection, one observation per call, and existing
acceptance/failure/drop counters. A saved result remains unchanged after stop.

Another 24 lifecycle cases cover never-started, connecting, executor-local
request-only stop, completed stop and restart for all configurations. The stop
callback observes Stopping before its posted cleanup can execute, then the
outside caller waits for actual completion and observes NotStarted. The saved
Stopping result stays unchanged after completion. The last cancelled-I/O
completion hook also verifies Stopping after the link has closed but before
cleanup completion is published.

The observer hook is internal and null by default. It runs at the public bool
adapter after the submission lock is released; it does not choose the outcome.

## Validation

- Full Debug build and CTest with -j2: 1131 discovered, 1119 passed,
  12 existing UDP diagnostic skips, zero failures.
- After adding the final cancelled-I/O completion assertion, all 48 admission
  result cases passed again.
- ASan with leak detection: all 166 selected TCP transport and admission/wait
  tests passed, including the strengthened lifecycle cases.
- TLS-enabled build: 134 selected cases passed, including TLS loopback.
- clang-format and git diff --check passed.

## Remaining work

Public SendResult API migration, wrapper lifecycle and precedence mapping,
other transports, custom channels, fanout aggregates and bindings remain.

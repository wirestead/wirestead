# TCP native admission results

Private TCP write helpers now return SendResult for copy, move and shared
buffers, including the try variants. The public Channel overrides still
return accepted(), and the wrapper's existing pinned calls do the same.
No public method signatures or Channel vtables change.

The helpers preserve the outcome at the point of decision:

- An unavailable or replaced native connection yields NotReady.
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
reported whole-queue size limit), distinguish NotStarted and Stopping from
native NotReady, and map capacity refusal according to call site and strategy.
BestEffort wrapper refusal must ultimately report QueueFull where specified.
Wait cancellation uses the connection record from PR #670 instead of these
immediate admission results.

Ordinary BestEffort native writes retain their existing deferred queue/drop
policy. Try writes retain their immediate refusal and existing dropped/failed
accounting. This change does not introduce new rejections, move payloads on
rejection, change retries, or alter post-acceptance accounting.

## Regression coverage

Twenty-four parameterized cases cover all six write forms, both strategies
and enabled/disabled pooling. They check unavailable state, invalid/oversized
payloads, acceptance, deterministic capacity refusal with executor progress
paused, caller storage after rejection, one observation per call, and existing
acceptance/failure/drop counters. A saved result remains unchanged after stop.

The observer hook is internal and null by default. It runs at the public bool
adapter after the submission lock is released; it does not choose the outcome.

## Validation

- Full Debug build and CTest with -j2: 1107 discovered, 1095 passed,
  12 existing UDP diagnostic skips, zero failures.
- ASan with leak detection: all 142 selected TCP transport and admission/wait
  tests passed, including the 24 new result cases.
- TLS-enabled build: 110 selected cases passed. The five TLS loopback cases
  also passed after explicitly rebuilding their executable with the new library.
- clang-format and git diff --check passed.

## Remaining work

Public SendResult API migration, wrapper lifecycle and precedence mapping,
other transports, custom channels, fanout aggregates and bindings remain.

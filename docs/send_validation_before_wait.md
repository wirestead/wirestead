# Payload validation before waiting

> Historical implementation-stage record. Public return types and custom-channel
> compatibility have since changed; see [the current client API](client_send_results.md).

This implements the empty-payload and per-message maximum subset of
C-3.1-1b in the [contract audit](communication_contract_v0.10_audit.md).
The [D-3 decision](communication_contract_v0.10_decisions.md) identifies
validation-before-waiting as an independent prerequisite for structured
acceptance results.

## Behavior

When queue pressure is active, Reliable sends and explicit blocking sends
used to wait before the transport could reject an invalid payload. An empty
raw payload or a payload above MAX_BUFFER_SIZE now bypasses that wait in
all seven wrappers. The same transport write path handles rejection, so
failure accounting and error callbacks are retained.

The common helper only decides whether size warrants a capacity wait. It is
not a second acceptance decision, and it does not move rejection accounting
into the wrapper. Existing null/empty shared-buffer rejection remains in
place. BestEffort and explicit try-send paths already do not wait and are
unchanged.

Newline-appending APIs are judged by the complete payload: an empty line is
one valid newline; a line of MAX_BUFFER_SIZE bytes becomes oversized after
the newline. Valid requests, including requests exactly at the per-message
maximum, still wait when pressure is active.

## Regression coverage

- Four injected client wrappers exercise all blocking-capable API forms with
  empty, null, oversized and delimiter-overflow inputs under active pressure.
  The channel validates and counts failed writes so early wrapper refusal
  cannot silently bypass the existing transport accounting.
- The same clients retain waiting for valid data, maximum-sized raw/shared
  payloads, maximum-sized newline-appended payloads, and empty lines.
- TCP, UDS and UDP server loopbacks hold a real session or channel under
  pressure. Invalid Reliable send_to, send_to_blocking and oversized
  send_to_line calls return without releasing that pressure.
- Server controls prove valid data and an empty line still wait until the
  queued write is allowed to finish.
- All test failure paths release pressure before joining senders.

Before the change, the TCP client and server invalid-input tests exceeded the
two-second bound; both valid-wait controls passed. A separately compiled
draft using the proposed wrapper changes passed all 14 tests across all
seven wrapper types.

## Local validation

- Full Debug build: cmake --build build -j2 passed.
- Full CTest: 950 discovered, 938 passed, 12 existing UDP diagnostic skips,
  zero failures (ctest --test-dir build --output-on-failure -j2).
- All 14 new tests passed under AddressSanitizer with libasan.so.8,
  ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 and CTest -j2.
- Repository clang-format, cmake-format and git diff --check passed.

## Scope limits

The public return type remains bool. This is not the full D-3 conversion.
Whole-queue-size limits, readiness ordering, connection-instance fencing,
stable waiter cancellation reasons and fanout result types are subsequent
work. The patch does not change allocation, ownership, retry policy, or
error reporting after transport validation.

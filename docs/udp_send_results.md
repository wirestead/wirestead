# UDP send admission and wait results

The built-in UDP client and server targeted sends now combine internal
SendResult decisions. Public send and native Channel methods still return bool;
acceptance describes local admission, not delivery.

## Readiness and validation

Wrapper copy, line, move/shared and server targeted sends validate payload shape,
MAX_BUFFER_SIZE and the available native hard queue limit before lifecycle,
target lookup or waiting. Rejection preserves move storage and bypasses native
failure/drop accounting when validation finishes at the wrapper.

A native UDP run must have opened its socket before accepting writes. Default
destination writes also need a configured or learned remote; explicit-target
writes only need the opened socket. Started-but-not-open and missing targets
are NotReady. Never-started/completed-stop is NotStarted, and requested but
incomplete cleanup is Stopping. A built-in wrapper must itself be started,
including when its injected native channel is already ready.

Native entry points retain typed validation, lifecycle and capacity decisions
under the submission mutex. Explicit try forms report WouldBlock for pressure.
Ordinary BestEffort wrapper forms map capacity refusal to QueueFull. Reliable
and explicit blocking forms retry only transient WouldBlock, at most five times.
Callback scopes never enter a capacity wait or retry after a capacity refusal.

## Wait identity and terminal causes

Each client wait captures the native run generation. Each server virtual session
retains a wait record tied to that run. Native stop, native terminal state and
server session expiration end records under the native submission mutex:

- Stop selects CancelledWhileWaiting.
- Native terminal error or session expiration selects NotReady.
- The first terminal cause wins and survives later stop, restart or replacement.
- Capacity release permits another synchronized state/run/target/admission check.

The server expires a record before removing its session under the wrapper lock.
A replacement session, including a reused numeric ID after restart, has a new
record. Final admission checks the wrapper generation, record identity and
native run; a parked old sender never submits into a replacement run.

Records are retained only by active senders/sessions. Native weak registrations
are pruned on capture and cleared on a terminal event. Result observers run
after admission locks release. Polling remains bounded to tolerate a lost
backpressure notification.

## Compatibility

Native writes issued immediately after start, before open completes, now reject
instead of queuing early. Callers must wait for readiness. Wrapper validation
can now prevent the native move/shared oversized-write diagnostic from running;
its counters and asynchronous Error callback are consequently not produced for
those early wrapper refusals. Native shape/MAX_BUFFER_SIZE validation also runs
before native lifecycle/capacity decisions. The existing native hard-limit
move/shared diagnostic remains for payloads within MAX_BUFFER_SIZE.

The existing UDP deferred OS message-size error policy remains: a datagram may
be accepted locally and fail later at the socket. This work does not introduce
a new UDP per-message size policy or promise delivery.

Custom bool-only Channels retain their fallback path without invented rejection
reasons. Broadcast/fanout remains its existing bool path, pending aggregation.
Serial, TCP/UDS server targets, public result-returning interfaces and bindings
remain follow-up work.

## Verification

Regression cases cover all client forms, both strategies, native lifecycle and
capacity, listening-without-peer versus explicit destinations, callback refusal,
first error/stop outcomes, wrapper/native restart, session expiration, ID reuse
and final admission into a replaced native run. Existing UDP transport tests
wait for readiness before exercising their original queue/error assertions.

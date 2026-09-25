# Serial send admission and device reopen

> Historical implementation-stage record. Public return types and custom-channel
> compatibility have since changed; see [the current client API](client_send_results.md).

The built-in Serial transport and wrapper retain SendResult internally.
Public send methods still return bool. Acceptance means local admission,
not delivery to the device.

## Validation and readiness

Wrapper copy, line, move and shared forms validate empty/null input, the
maximum payload size and the native hard queue limit before lifecycle or
capacity checks. The line delimiter counts toward the limit. Rejected moves
retain their storage. Early wrapper refusals bypass native failure counters.

The wrapper must be started even when an injected native Serial is already
connected. Never-started and fully stopped objects report NotStarted;
incomplete stop reports Stopping. Native writes require a successfully opened
and configured device; opening, reopening and terminal errors report NotReady.
Native classification, connection checks and reservation/submission share
the submission mutex.

## Capacity and wait outcomes

Explicit try forms report WouldBlock for capacity refusal. Ordinary
BestEffort wrapper sends map it to QueueFull. Reliable and explicit blocking
forms retry only native WouldBlock, at most five times. Callback scopes never
wait for capacity or retry a refusal.

Each successful device opening owns a wait record. Stop records
CancelledWhileWaiting and device loss records NotReady. The first terminal
event wins under the native submission mutex and survives later reopening or
stop. Only capacity release permits another admission check. Final native
admission checks the pinned opening, so a waiting sender cannot send to a
replacement device connection.

## Reopen and buffer lifetime

Accepted writes capture both the run and device opening. Loss discards queued,
pending and active writes and records drops. Posted old ordinary writes release
their own inflight reservations; posted try writes must not subtract the
reservation already removed by the old opening's drain.

Reads and gather writes retain their buffers until their own completions.
Late completions from an old opening do not alter new queue accounting,
write state, receive callbacks or sent statistics. Idle timer completions also
check the opening. Bytes already delivered cannot be retracted.

Existing EOF handling, reopen policy, line settings and Serial's fatal queue
overflow policy remain unchanged.

## Compatibility and remaining scope

Native pre-open sends now reject instead of waiting in a queue for open.
Old accepted data is no longer replayed after device loss. An injected connected
native port still requires wrapper start. Invalid wrapper sends are rejected
before native accounting, and ordinary native payloads above MAX_BUFFER_SIZE
are explicitly refused.

Custom bool-only Channels retain their existing path without invented typed
reasons. Public result APIs, bindings, TCP/UDS server targeted sends and fanout
aggregation remain separate work.

## Verification

Device-independent fake-port tests cover all six native forms, both queue
strategies, pool on/off, validation/state/capacity precedence, moved-storage
preservation, bounded retries, callback refusals, stop/loss ordering, reopened
capacity, final admission and delayed gather completions. Delayed ports borrow
the actual gather views so ASan checks their buffer lifetime.

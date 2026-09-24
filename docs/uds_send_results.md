# UDS send admission and wait results

The built-in UDS client now combines native admission and wrapper decisions
internally as SendResult. Public Channel and wrapper methods still return
bool. Acceptance means local admission, not delivery.

## Validation and lifecycle

All built-in wrapper copy, line, move and shared forms validate empty/null data,
MAX_BUFFER_SIZE and the available native hard queue limit before checking
lifecycle or waiting. A line's delimiter counts toward the limit. Rejection
does not consume move storage. These early wrapper refusals do not increment
native failure/drop counters.

A wrapper must be started, even if an injected native UDS client is connected.
A stop call in progress, an admitted callback still running after a stop
request, or native cleanup not yet completed yields Stopping. A completed
stop yields NotStarted; a started but disconnected client yields NotReady.
Native classification, connection checks and reservation/submission share the
submission mutex. Internal result hooks run after the admission locks release.

## Capacity and first terminal cause

Explicit try forms return internal WouldBlock for insufficient capacity.
Ordinary BestEffort wrapper forms map that refusal to QueueFull. Reliable
and explicit blocking forms retry only transient native WouldBlock, at most
five times. Callback scopes never enter a capacity wait or retry after refusal.

Each successful native connection owns a wait record. Stop selects
CancelledWhileWaiting; connection loss selects NotReady. The first event
wins under the submission mutex, and a waiter retains that record through
reconnect or restart. Only a capacity release permits another admission check.
Final admission checks the pinned connection, so a sender cannot switch peers
between its wait and its write.

UDS ordinary native writes continue to reserve the hard cap under both
strategies. The existing native bool entry points adapt typed decisions;
wrapper prevalidation and strategy mapping are separate stages.

## Reconnection and buffer lifetime

Every accepted write captures the native run and connection. On loss, queued,
pending and active batches are discarded and counted as drops. Posted old
submissions are discarded on arrival and release their own inflight reservation.
Old try reservations were removed by the connection drain and must not subtract
from a replacement connection's queue.

Read buffers and gather-write batches live through their own completions.
A late completion from an old connection releases its buffers without changing
the new connection's queue, write-in-progress flag or sent statistics. Data
already sent before loss cannot be retracted; this is not a delivery guarantee.

## Compatibility and remaining scope

This changes behavior: old connection data is no longer replayed after reconnect,
an injected connected native client still needs wrapper start, and early invalid
wrapper sends bypass native counters. Native ordinary writes also reject data
above MAX_BUFFER_SIZE explicitly.

Custom bool-only Channels keep their existing admission path without fabricated
typed rejection reasons. Public result-returning APIs, UDP/Serial, server
targeted sends and fanout aggregation remain separate work.

## Verification

Tests cover wrapper validation/state/capacity precedence, all send forms,
callback refusals, bounded retries, stop/loss ordering, reconnection with both
free and pressured capacity, final-admission loss, and delayed old completions.
The delayed socket tests borrow actual gather views, cover all six native forms
with pooling on/off and both strategies, and check drop/sent/queue accounting.

# Post-acceptance accounting and event proposal

**Status: accounting and lifecycle event policy implemented.** The
[stream accounting implementation](tcp_send_accounting.md) now implements
the TCP/UDS client/session and Serial request ledger with explicit measurement epochs and cause groups.
[UDP socket accounting](udp_send_accounting.md) also implements these totals.
TCP/UDS session aggregation and UDP virtual-session accounting are implemented.
The selected UDP expiry policy discards waiting requests and preserves active
completion outcomes. The approved event choices below are implemented; see [lifecycle events](lifecycle_events.md).
The [current conformance report](communication_contract_v0.10_status.md)
identifies the missing behavior. This document specifies implementation gates.
Existing SendResult/FanoutResult admission semantics do not change.

## Requirements already decided

Acceptance is separate from delivery. Later stop/loss cannot revise an accepted
return value. Discarded-before-write and aborted-during-write requests must be
counted separately, with no per-request error callback. Old connection data
must not be replayed after reconnect. Reliable queue pressure must not remove
accepted requests.

The implementation documents define counter names and reset epochs. UDP expiry
now has an explicit queued-versus-active decision; the expiry event is on_session_expired.

## Proposed request accounting

Assign one logical record at the synchronized acceptance decision, before
posting work. It belongs to the original run, connection/session identity and
statistics epoch. Fanout creates one record per accepting target; rejected
targets create none. A gather batch retains each request's identity.

| Stage | Meaning | Terminal classification on cleanup |
| --- | --- | --- |
| Admitted, pending post | Accepted but not processed by executor | Discarded before write |
| Queued | Waiting for local I/O | Discarded before write |
| Active | Handed to local I/O, including a composed gather | Written if fully completed; otherwise aborted during write |
| Terminal | Written, discarded or aborted | No further accounting |

Mark Active before initiating I/O, including initiation failure handling.
Returning an active batch to a queue on error must not erase its Active history.
Shutdown must account for pending posts as well as executor queues.

For gather completion, use reported byte offsets and request boundaries.
Requests fully covered by the completed prefix can be written; unfinished
requests handed to the operation abort on terminal failure, even when zero
bytes are confirmed for a suffix. Requests not handed to I/O discard.
Validate this interpretation against each composed I/O primitive before
finalizing it. Local completion never proves peer delivery.

Use exactly one terminal transition. Late completions and stop/loss races
must not double count or update a replacement connection. Proposed causes:
explicit stop, connection/socket loss, virtual-session expiry. Pressure disposal
needs a separate cause only if an opt-in policy is adopted. Preserve the first
terminal cause rather than replacing loss with a later stop.

Proposed counters report logical request counts and full payload sizes per
terminal class/cause. Confirmed local-I/O bytes are a separate measure.
Existing sent_messages can count I/O batches and must not be silently relabeled.

At a quiescent point within one statistics epoch:

    accepted requests = written + discarded + aborted + outstanding

Live snapshots remain observational. Reset/restart must fence old epochs.
If explicit reset can occur with outstanding requests, specify a carried-in
count or another explicit rule before promising conservation. Session removal
folds totals into the server exactly once. Preserve existing failed_sends and
dropped semantics or document a versioned compatibility change; neither can
reconstruct this ledger alone.

## Transport-specific work

TCP, UDS and Serial clients now retain records spanning caller admission,
pending posts, queues and active writes. TCP/UDS sessions now provide exactly-once server
aggregation, including stop retirement and reset. UDP now retains a record per admitted datagram and socket-run identity;
its shared socket snapshot is distinct from per-virtual-session statistics.

UDP datagrams carry a retained virtual-session token in addition to destination
and socket run. Expiry discards waiting work under the admission mutex, leaves
active work to complete, and removes only that token's physical queues on the
strand. client_stats exposes live peer projections; socket totals retain expired
contributors without a second aggregation step. Endpoint reuse gets a new token.

## Approved event choices

- Established connection loss emits one disconnect even if retry succeeds.
  Retry attempts are queryable state, not per-attempt on_error.
- Terminal start/retry failure emits one terminal error. Loss followed by
  exhaustion can produce two distinct state events, not per-request errors.
- Explicit stop suppresses disconnect; external stop remains the completion
  boundary, consistent with the current wrapper callback gate.
- UDP expiry has a distinct event/reason, never asserting remote disconnect.
  Waiting endpoint work now expires; active work keeps its completion outcome.
  The concrete UDP server exposes on_session_expired.
- Wrapper callback exceptions are logged without recursive on_error.
  Direct native callbacks need their own documented boundary.
- Configuration failure timing and exception/result delivery remain separate.

Connect/receive/disconnect ordering and serialization need multi-thread evidence
before promising this event sequence. D-1 shutdown does not prove ordering.

## Acceptance scenarios

Run applicable cases independently on all four clients and three servers.
Use deterministic gates rather than timing-only sleeps.

1. Accept, pause before posted enqueue, then stop/lose the connection: one
   pre-write discard, with the accepted result unchanged.
2. Queue behind a held write, then stop/lose: queued requests discard; active
   work aborts unless completion proves it fully written.
3. Complete a gather prefix and fail mid-request: preserve logical boundaries,
   confirmed bytes and exactly-once classification.
4. Race loss/stop, deliver late cancellation, then reconnect: keep the first
   cause, never replay or affect the replacement run.
5. Repeat with reset/restart epochs, zero-byte completion, initiation failure
   and session removal; assert quiescent accounting and server totals.
6. Reject invalid/oversized/unready/pressured input: no accepted-loss counts,
   move consumption or retained shared input.
7. Mixed fanout admission followed by different per-target outcomes:
   the returned aggregate never changes.
8. Expire a UDP virtual session with pending/active datagrams while another
   endpoint continues; cover endpoint reuse and the selected expiry policy.
9. Recovered loss, retry exhaustion, explicit stop and callback exceptions:
   verify event counts and multi-thread ordering separately.

Counter compatibility, reset semantics and UDP expiry are documented in the
implementation contracts. Event regression and platform verification are completion gates, not
assumptions supplied by passing the existing suite.

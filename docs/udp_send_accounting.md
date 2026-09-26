# UDP socket and virtual-session post-acceptance accounting

The built-in UDP channel exposes RuntimeStats::send_accounting. UDP client and
server wrappers expose this socket-wide snapshot. UDP server client_stats(id)
additionally exposes a live virtual session, identified by its retained session
token rather than only its endpoint address. Unknown, expired and stopped IDs
return nullopt.

Each accepted datagram, including an explicit-destination send, gets a unique
request identity before its enqueue handler is posted. Pooled/fallback copy,
move, shared, try and run-pinned paths use the same ledger. Request counts and
full payload byte counts satisfy:

    accepted = written + outstanding
             + all discarded_before_write + all aborted_during_write

The counter fields, first-termination rule and measurement epochs are described
in [the shared accounting contract](tcp_send_accounting.md). UDP writes contain
one datagram rather than a stream gather batch. Written means successful local
completion of that datagram, not peer receipt or application processing.

## Causes and boundaries

- explicit_stop: stop terminates every accepted identity, including pending
  posts. Work not handed to local I/O is discarded; an active datagram is aborted.
- connection_loss: for UDP this existing field means a terminal **local socket
  error**, not a detected peer disconnect. Send failure, receive failure and
  initiation exceptions terminate the run's outstanding work and close further
  admission. Terminal states also drain queued/pending storage.
- session_expiry: virtual-session expiry discards accepted work that has not
  started, including posted enqueue handlers and queued/pending datagrams.
  An active datagram retains its actual completion, stop or local-error outcome;
  expiry never cancels the shared socket or an unrelated endpoint.
- queue_pressure: retained for defensive post-admission queue rejection. Default
  fixed-limit routing now preserves accepted datagrams. An ordinary try refusal never
  creates an accepted request or a post-acceptance loss.

The active boundary is immediately before async_send_to initiation, serialized
with admission, stop and virtual-session expiry. Initiation exceptions are active aborts, even when no
bytes reached the operating system. A datagram larger than the protocol allows
can be accepted by the existing general payload-size check and then fail at
local I/O; that failure is accounted as an abort.

confirmed_written_bytes records locally reported completion bytes only while
the request is still outstanding. A completion after stop/error cannot revise
its terminal classification. Short successful datagram completions are treated
as local errors. No per-request error callback or delivery acknowledgement is
introduced.

## Reset, restart and scope

reset_stats starts a new measurement epoch; older work can still transmit, but
its later completion/cleanup is excluded. Request IDs never repeat. Stop waits
for native operation completion before the next socket run, and write completions
also retain their originating run identity.

The UDP server's shared socket totals include sends to all destinations,
including direct native sends without a virtual-session token. Each tracked
request updates its peer projection and the socket ledger under one ledger
mutex. Removing a session does not copy or add its totals again: the socket
already owns them. A late active completion remains in the socket aggregate,
and cannot update a replacement virtual session at the same endpoint.

The selected expiry policy is **discard waiting work, finish active work**.
Expiry ends admission/waiting immediately; physical queue cleanup runs on the
transport strand. The existing on_disconnect expiry callback is unchanged and
does not prove remote disconnection. A distinct expiry event remains separate
policy work.

Peer legacy accepted/sent/receive/drop/failure counters and queue gauges describe
tagged traffic. Backpressure is shared socket capacity: backpressure_active and
backpressure_events report the socket state in every peer snapshot and must not
be summed across peers. Posted plain-write reservations are not yet included in
queued_bytes/pending_bytes, consistent with the native gauges. SendAccounting
already includes accepted posts.

reset_stats resets both socket and peer ledger epochs, including retired active
requests; old completions cannot enter the new ledger epoch. Legacy counters
remain observational and are not atomic with the ledger. stop retains cumulative
socket totals after destroying a factory-created channel; another stop does not
erase them. A wrapper restart begins fresh totals for either owned or injected
native channels. Native channel restart alone still requires explicit reset.

**ABI change:** SendAccounting appends session_expiry, changing its size and
RuntimeStats. Rebuild C++ consumers, including direct session users whose layout
embeds ledger state. The new cause is zero for transports without virtual-session
expiry; session_expiry.aborted_during_write stays zero under this policy.

## Verification

The accounting tests exercise ten input configurations: seven default-peer
forms and pooled/fallback/try explicit-destination sends. They cover local
success, stop before enqueue and after initiation, initiation failure, a real
oversized-datagram send failure, an injected terminal receive error, reset
before enqueue/during I/O, restart, rejected admission, BestEffort queue preservation and
Reliable pending queues. Failure assertions include request/byte conservation.
Virtual-session cases cover try and pooled/fallback blocking admission, per-peer
receive/send totals, fanout, shared-capacity rejection, reset, stop/restart with
both channel ownership modes, local error, endpoint reuse and two-thread expiry
while a write is held active. A controlled clock selects one expired peer while
a second remains live; posted and Reliable pending work are checked separately.
See [current policy coverage](communication_contract_v0.10_status.md) for the
remaining execution-scope and event-policy gaps.

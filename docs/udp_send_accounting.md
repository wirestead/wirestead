# UDP socket post-acceptance accounting

The built-in UDP channel exposes RuntimeStats::send_accounting. UDP client and
server wrappers forward this socket-wide snapshot. It is not a per-peer or
virtual-session snapshot: UDP server client_stats and expiry accounting remain
follow-up work.

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
- queue_pressure: existing keep-latest trimming and post-admission queue
  rejection classify only the removed requests. An ordinary try refusal never
  creates an accepted request or a post-acceptance loss.

The active boundary is immediately before async_send_to initiation, serialized
with admission and stop. Initiation exceptions are active aborts, even when no
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

The UDP server's shared socket totals include sends to all destinations.
Virtual-session expiry does not currently remove queued datagrams for that
endpoint. This extension does not choose a new expiry/event policy or cancel
unrelated peers on virtual-session removal.

Legacy sent/dropped/failed counters retain their separate meanings; the new
ledger is a consistent snapshot but is not atomic with legacy fields.
There are no new public fields beyond the existing optional SendAccounting.
Consumers already rebuilt for that layout do not need another ABI migration.

## Verification

The accounting tests exercise ten input configurations: seven default-peer
forms and pooled/fallback/try explicit-destination sends. They cover local
success, stop before enqueue and after initiation, initiation failure, a real
oversized-datagram send failure, an injected terminal receive error, reset
before enqueue/during I/O, restart, rejected admission, keep-latest disposal and
Reliable pending queues. Failure assertions include request/byte conservation.
See [current policy coverage](communication_contract_v0.10_status.md) for the
remaining server-session and event-policy gaps.

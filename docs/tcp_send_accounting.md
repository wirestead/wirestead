# Stream client post-acceptance accounting

The built-in TCP and UDS clients and Serial transport expose logical-request accounting through
RuntimeStats::send_accounting. It is an optional SendAccounting snapshot:
nullopt means the transport does not implement it, not that it has zero loss.
UDP socket accounting is covered in [its transport-specific contract](udp_send_accounting.md).
TCP/UDS server-session accounting and aggregation remain follow-up work.
The corresponding wrappers forward their native channel snapshots unchanged.

## Public counters

SendRequestTotals contains requests and bytes (the full payload sizes).
SendAccounting contains accepted, written and outstanding totals, plus these
cause groups:

| Group | Discarded before write | Aborted during write |
| --- | --- | --- |
| explicit_stop | Accepted but not handed to local I/O when stop takes effect | Handed to local I/O but not completed when stop takes effect |
| connection_loss | Same distinction at connection loss | Same distinction at connection loss |
| queue_pressure | Accepted request removed by existing keep-latest/queue routing | Zero for current stream-client routing; active writes are not trimmed |

Each group is a SendLossTotals with discarded_before_write and
aborted_during_write members. Rejected admissions do not contribute to these
totals. Ordinary BestEffort try refusal therefore differs from the existing
plain-write keep-latest disposal; this change does not adopt that queue policy
as the final v0.10 policy.

The request's acceptance result never changes. There is no per-request error
callback. Written means local completion, not peer receipt or processing.

## Handoff, completion and termination

Each accepted request gets a unique identity before its posted enqueue handler.
The identity remains attached through pending storage, queue trimming and gather
writes. Copy, pooled copy, move, shared, try and connection-pinned paths all use
the same accounting. Reusing a shared payload is still a separate request for
each admission.

The local-write boundary is immediately before asynchronous initiation. Failure
to initiate after that boundary aborts the active request and closes the
connection, discarding the remaining queued/pending requests. Stop and connection
loss serialize with admission; they terminate all outstanding identities,
including requests whose posted handlers have not run. First termination wins.

A gather completion's byte prefix is distributed over its original request
boundaries. Fully covered requests count as written; unfinished requests handed
to that operation count as aborted on failure, even if zero bytes are confirmed
for a suffix. confirmed_written_bytes records the local bytes reported before
termination. A completion arriving after stop/loss cannot revise the final
classification or confirmed byte total. Thus an abort can include data already
in the kernel or peer; it is not proof that no bytes were sent.

The ledger snapshot itself is taken under one mutex. Within that snapshot,
request counts and full-payload byte counts satisfy:

    accepted = written + outstanding
             + all discarded_before_write + all aborted_during_write

Legacy RuntimeStats fields are still independently read atomics and must not
be combined with this identity. Legacy messages_sent counts I/O operations,
which can contain several requests; dropped/failed counters retain their
existing meanings. A post submission that throws rolls back its ledger entry
rather than leaving a phantom accepted request.

## Reset and lifetime

reset_stats starts a new measurement epoch. Only requests admitted in that
epoch appear in the new totals. Previously accepted work can still transmit,
but its subsequent completion or disposal is excluded from the new epoch.
This avoids nonzero completions with zero accepted requests after reset;
outstanding is consequently the current epoch's outstanding requests, not
a replacement for physical queue_bytes/pending_bytes.

Identifiers are not reused across reset or reconnect. Cancellation completions
cannot mutate new requests. Wrapper stop/start continues to reset statistics
under the existing wrapper lifecycle contract. Native reset remains explicit.

The tracker adds one metadata entry per outstanding request and uses a mutex
for admission/transition/snapshot. It retains no additional
payload copy. Stop traverses outstanding metadata. No throughput improvement
is claimed; performance tuning remains separate from correctness evidence.

## Compatibility and verification

RuntimeStats has a new optional member, changing its binary layout. Rebuild C++
consumers together with the library. Source users of existing fields keep their
semantics. Python's existing bound statistics/API are not extended here.

Ledger unit tests cover partial gather prefixes, terminal races, first cause, reset
epochs, replacement-connection identity, rollback and unsupported capability. TCP loopback tests cover every
admission family, stop before enqueue, stop after handoff with late completion,
gather request counts, real peer loss, reset, rejected inputs, keep-latest versus
try refusal, and injected initiation failure.

UDS/Serial controlled-interface tests cover seven input families on each
transport: pooled/fallback copy, move, shared and all three try variants.
They verify pre-enqueue stop, active versus queued loss, partial gather prefix,
inline interface completion, initiation exceptions, reset before enqueue and
during I/O, stale completions after reconnect, write EOF/short completion,
rejected admission and keep-latest disposal. These tests do not require a
physical serial device. Existing real-I/O tests remain regression coverage.

UDS/Serial write completions are posted to the transport strand even if an
injected interface invokes them inline. This permits admission/stop and write
initiation to share one mutex without reentrant completion deadlocks.
A short composed write without an error is treated as connection loss.
Serial read EOF keeps its existing transient retry behavior; write EOF instead
terminates the device instance, following the configured reopen policy.
This extension adds no public fields beyond the optional member introduced
with TCP accounting. UDP socket accounting is documented separately; TCP/UDS server aggregation
and per-virtual-session UDP accounting remain unsupported.

See [current policy coverage](communication_contract_v0.10_status.md) and the
[remaining accounting/event proposal](post_acceptance_policy_v0.10.md).

# Built-in receive-memory limits

The seven concrete wrappers expose stopped-only receive_limits(ReceiveLimits)
and observational receive_stats(). Existing IFramer, MessageContext,
RuntimeStats, ChannelInterface and ServerInterface virtual APIs and layouts are
unchanged. Built-in framer friend access adds no data or virtual members.

## Limits and accounting

Defaults are a 64 MiB aggregate receive budget, a 1 MiB built-in framing-buffer
limit, and 1,024 reserved server session slots. A client reserves one scope.
The aggregate limit is per wrapper, not process-wide. All values must be
positive and the frame limit must fit the aggregate limit. These limits can be
set on the built object before start, or after an external stop has completed.
Changing them during a run or from a wrapper callback throws before applying
the new configuration.

The budget covers retained data/message payloads, conservative batch/staging
record allowances, and built-in framing-buffer storage and transactional working
copies. Reservations precede these allocations. Temporary replacement buffers
are charged alongside the buffers they replace. Record allowances cover vector
growth and staging; reserved_bytes is consequently a conservative accounting
quantity, not an allocator or process-RSS measurement.

The fixed native read buffer is separately bounded by the transport's existing
read-buffer configuration. Socket/kernel storage, allocator bookkeeping, user
callback allocations/copies, configured patterns/callback objects and arbitrary
custom framer internals are outside this budget. Session-count limits bound
wrapper session records/timers separately from the byte budget. This is not a
claim that the whole process consumes at most max_bytes.

The framing-buffer limit includes retained prefixes and delimiters. It is
separate from a framer's protocol-specific maximum payload length. The default
leaves room for the existing 64 KiB payload maximum plus its prefix. Existing
invalid-frame discard/resynchronization rules are unchanged.

Reservations follow queued batches into callback execution. Removing a session
or requesting stop inside a callback cannot release the reservation while the
delivered batch still exists. Completed external stop releases queued receive
storage. Changing the limits after stop starts a new receive accounting instance.

A new wrapper run resets receive event counters; reconnects within that run do not.
reset_stats resets receive event counters and rebases the peak to current usage;
it does not forgive outstanding storage. Snapshots are observational, not atomic
with legacy RuntimeStats or concurrent counter reset. Session slots include
retiring scopes whose storage is still in flight, not just connected peers.

## Overflow policy

- TCP/UDS clients and server sessions end the affected native connection using
  the connection-loss path. Existing retry configuration remains in effect.
- Serial ends the current device connection and follows reopen_on_error.
- UDP drops the new input. Other virtual sessions, previously queued batches
  and the previous built-in partial frame remain intact.
- Exceeding server session slots rejects only the new session. A rejected peer
  is not announced as a connected wrapper session.
- receive_stats records byte-limit, frame-limit, session-limit and allocation
  failure causes separately. Session-slot rejection has no retained payload
  size to charge; its event is still counted.
- Send accounting is unchanged. Receive overflow counters are not send drops.

For built-in framers, each input is parsed into temporary state and staged
messages before user receive callbacks become visible. This prevents an
quota-rejected UDP datagram from exposing only its first decoded messages or
destroying a previously accepted partial frame. The staging/copy cost is charged
to the same aggregate budget. A very small budget may therefore reject an input
that would fit if its temporary and retained storage were considered separately.

Allocation failures are counted and contained at the receive boundary. Unlike
quota rejection during preparation, a later allocator failure while transferring
a prepared message into a batch does not promise rollback of callbacks or parser
state already committed.

## Compatibility boundary

Existing custom IFramer implementations continue to compile without a new pure
virtual method or new extension interface. Exact built-in types receive the
framing-storage guarantee. Custom classes, including subclasses overriding
built-in behavior, retain their legacy parsing path; their internal memory and
transactional framing behavior are not covered. Wrapper-owned queued payloads
remain budgeted. Native overflow-close behavior requires the built-in transport;
the library cannot impose a close protocol on an arbitrary injected channel.

Client framer replacement keeps its existing API: an in-flight receive retains
its original framer and reservation owner. Existing builders can be used normally;
call the concrete wrapper's receive_limits before start (disable builder auto-start
when customizing limits).

Finite defaults can reject workloads that previously retained more receive
storage. Applications can raise the documented limits while stopped. This is
a behavioral compatibility change; it is not a universal new custom-framer
contract or a total-memory sandbox.

## Verification

Unit cases exercise aggregate reservation, size overflow, retiring scope
lifetime, whole-input framing rollback, staged callback visibility, adoption
into a batch without a second reservation, and the old maximum length-prefix
payload under the new defaults.

Native-backed wrapper tests cover all seven targets, isolated connection loss,
UDP retained-state preservation, Serial reopen, session-count rejection, reset
with outstanding data, stop from a callback and completed external-stop cleanup.
Normal, sanitizer and platform CI outcomes are recorded with the pull request.

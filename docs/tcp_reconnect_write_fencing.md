# TCP accepted writes across reconnect

TCP now identifies a usable connection separately from its start/stop run.
Ending that connection invalidates its generation under the admission mutex
introduced in PR #665. All six write forms capture that generation when
accepted. A posted write from the old generation is discarded, even when a
replacement connection is already ready.

## Queues, operations and accounting

Connection loss and idle-timeout closure drain the transmit and Reliable
pending queues and detach the active write batch. They reset the old queue
reservations and pressure state before publishing the next state. Ordinary
write reservations that have not reached the strand are released by their
stale routing handlers; stale try-writes never subtract from a new queue.

Each async write owns its batch and buffer views through completion. Detaching
an old batch therefore does not free memory still borrowed by Asio. Stale write
completions cannot subtract new queue bytes, reset new writing state or trigger
another close. Read buffers are also per connection, and read/idle-timer
callbacks carry the same generation guard. The connection-completion write
kick does not reset a write started by a Connected callback.

Accepted buffers abandoned on loss are recorded once as dropped messages and
bytes. For an unfinished batch, this counts the whole abandoned logical buffer:
some bytes may already have reached the old peer. The counters do not prove
non-delivery, and the library does not replay those buffers on the new peer.
Explicit stop retains its existing queue-cleanup contract.

This changes the old reconnect replay behavior. Public methods still return
bool; public signatures and ABI do not change.

## Regression evidence

Twelve tests submit a write from a receive callback and then end that connection
before its posted routing handler runs. They cover copy/move/shared, their try
forms and Reliable/BestEffort. The replacement peer must receive only its new
payload, and the old payload must be counted as dropped.

Two additional cases keep a large real socket write unfinished and put another
buffer in either the transmit queue or the Reliable pending queue. On reconnect
both old buffers are dropped, and the new peer receives only the new payload.
The original thirteen cases failed before the fix; the transmit-queue variant
was added during validation. No production test hook was added.

## Validation

- Full Debug build and CTest with -j2: 1029 discovered, 1017 passed,
  12 existing UDP diagnostic skips, zero failures.
- AddressSanitizer with leak detection: 64 selected TCP tests passed.
- TLS-enabled build: 42 selected TCP/TLS tests passed, including the new
  plaintext reconnect cases in a TLS-enabled binary.
- clang-format and git diff --check passed.

The new loss regressions use plaintext loopback sockets. The TLS tests verify
the existing encrypted paths; they do not exhaustively schedule TLS reconnect
cancellation races. No throughput benchmark was run.

## Remaining work

Wrapper callers that wait for capacity still need to pin the connection they
waited on and retain a stable stop/loss reason. This transport fence applies
after acceptance; it does not solve that pre-admission wait. Public SendResult
state/capacity reasons, other transports, fanout results and bindings remain
D-3 work. Per-batch ownership adds an allocation; throughput was not benchmarked.

# TCP write admission and stop

TCP client write admission now shares a submission mutex with explicit stop
requests. Previously a producer could pass its state check, pause, and reserve
capacity and register a write after another thread completed stop cleanup.

## Guarantee and callback behavior

For copy, move and shared writes, including their try-write forms, the state
check, capacity reservation, acceptance accounting and strand submission are
ordered before or after the stop request. A write ordered after the stop
request rejects. An admitted write has already registered its strand work
before stop can register cleanup. Acceptance still does not guarantee delivery.

Routing work uses post rather than dispatch. Writes originating on the strand
therefore defer routing and its callbacks until the current handler returns.
The bool admission result remains synchronous. In particular, a backpressure
callback may request stop without recursively acquiring the submission mutex.
Stop cleanup and completion waiting happen outside that mutex.

## Validation

- Before the fix, all six new write/stop race cases failed: stop completed
  while the producer was paused between its state check and reservation.
- After the fix, the six cases pass and verify acceptance accounting,
  drained queue/pending bytes and rejection after stop.
- A seventh regression test calls stop from a backpressure callback triggered
  by an executor-origin write and checks that both calls return.
- Full Debug build with -j2 passed; CTest discovered 1003 tests, with 991
  passed, 12 existing UDP diagnostic skips and zero failures.
- AddressSanitizer with leak detection passed all 15 tests selected by
  TcpWriteStopAdmission or TcpStopAdmission.
- clang-format and git diff --check passed.

## Scope and remaining work

This change synchronizes TCP client admission with explicit stop only.
The [readiness follow-up](tcp_write_readiness.md) synchronizes usable
connection state with admission. The [reconnect follow-up](tcp_reconnect_write_fencing.md) adds a transport
connection identity and discards accepted old data. Wrapper capacity waiters
still need to pin that identity. Stable wait cancellation, structured state/capacity
rejection reasons, public SendResult returns, fanout results and bindings
remain D-3 work. Concurrent start/stop is not added as a supported operation.
No public signature or ABI changes are introduced here.

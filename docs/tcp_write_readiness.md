# TCP write readiness

TCP client admission now requires a usable connection. Copy, move and shared
writes, including all try-write forms, reject before start, while connecting
or performing the TLS handshake, and after connection loss. Rejection does not
reserve capacity or count the message as accepted. Existing failed-send
accounting applies, including BestEffort writes rejected for lack of readiness.

## Synchronization and compatibility

Readiness publication and invalidation share the submission mutex introduced
in PR #665. The readiness check remains under that mutex through reservation,
acceptance accounting and strand submission. Connection completion checks the
run and stop flags before publishing readiness. State callbacks run outside
the mutex, so a Connected callback can send and a Connecting callback rejects
without deadlocking.

This intentionally removes the old offline-queue behavior. Applications must
wait for a usable connection before submitting data. Public methods still
return bool and no public signature or ABI changes here.

## Evidence

Twelve new parameterized regressions cover all six write forms: rejection
before start without retained work, rejection in initial Connecting and
connection-loss callbacks, and successful admission inside Connected callbacks.
All twelve failed before the production fix and pass afterwards.

Existing backpressure, try-write accounting, invalid-payload and callback
exception tests now establish real loopback connections instead of depending
on offline acceptance. Tests reserve capacity before driving the executor or
request stop from the pressure callback to avoid depending on socket drain
timing.

Validation: full Debug build and CTest passed with -j2 (1015 discovered,
1003 passed, 12 existing UDP diagnostic skips). After adding the Connected
callback acceptance assertions, all 12 readiness regressions passed again.
AddressSanitizer with leak detection passed all 46 selected TCP readiness,
stop admission, transport, try-write and backpressure contract tests.
clang-format and git diff --check passed.

## Remaining work

A readiness check does not identify a connection instance. A sender waiting
across disconnect/reconnect still needs generation fencing and stable release
reasons. The [fencing follow-up](tcp_reconnect_write_fencing.md) discards previously
admitted TCP data across connection loss. This readiness change alone did not
implement that guarantee.
Public SendResult state/capacity reasons, fanout results and bindings remain
D-3 work. Concurrent start/stop is not added as a supported operation.

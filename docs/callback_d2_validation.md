# D-2 callback blocking-send validation

D-2 is defined in [the contract decisions](communication_contract_v0.10_decisions.md).
The implementation adds a depth-counted guard to the common wrapper callback
invocation. A send with capacity available still follows normal acceptance;
a send that would wait returns false while a user callback is running.

## Scope audit

All seven wrappers route every listed event through
detail::invoke_user_callback, including shared handler snapshots and both
size-triggered and timer-triggered batch delivery.

| Wrappers | Callback paths audited |
| --- | --- |
| TCP client/server | data, message, both batch forms, connect, disconnect, error, backpressure |
| UDS client/server | data, message, both batch forms, connect, disconnect, error, backpressure |
| UDP client/server | data, message, both batch forms, connect, disconnect, error, backpressure |
| Serial | data, message, both batch forms, connect, disconnect, error, backpressure |

Existing receive-envelope guards are retained to preserve their prior scope.
The depth counter makes nesting safe; a common invocation's guard is restored
before exception logging without clearing an outer callback's scope.
D-1 admission gates and target-executor shutdown decisions are unchanged.

The loopback tests also exposed a missing UDS transport connection: the
server stored its backpressure handler but never subscribed to session
pressure transitions. Sessions now forward those transitions through the
current server handler, with generation/liveness checks and the handler
snapshot taken under the session mutex before invocation outside it.

## Regression coverage

- Four injectable client wrappers each exercise ten dispatch paths: the eight
  event kinds above, plus both timer-delivered batches. Each callback checks
  all six blocking-capable API forms with capacity and under pressure, using
  a distinct healthy target so disconnect/error cannot hide missing guards
  behind a readiness rejection. Disconnected/error source sends also reject.
- TCP, UDS and UDP server loopbacks exercise connect, disconnect, data,
  message and both batch triggers. Each dispatch sends through all three
  server forms (Reliable send_to, send_to_blocking and send_to_line) to
  another server held under pressure. Ready self sends accept; sends to a
  disconnected session reject.
- Server error callbacks are exercised through injected state errors for
  TCP/UDS and an actual occupied UDP bind port. They also send to another
  pressured server.
- Each server's actual backpressure callback rejects all three forms. An
  outside caller remains blocked while that callback holds the executor;
  releasing it permits the queued write to complete and the outside send
  to succeed. All three outside send forms are checked per server.
- Each client's outside caller still waits for pressure to clear after a
  throwing callback. Nested and shared callback invocation, empty handlers,
  non-standard exceptions and thread-local isolation are covered.
- Regression cleanup releases pressure instead of leaving test processes
  deadlocked when a callback guard is missing.

Before the change, the client matrix failed for all four wrapper types while
the four outside-wait controls passed. The server callback tests also
reproduced failures across TCP/UDS/UDP; the strengthened TCP error case waited
for the five-second watchdog before accepting a send that should reject.

## Local validation

Validated on Ubuntu 24.04 under WSL, GCC Debug with TLS disabled:

| Check | Result |
| --- | --- |
| cmake --build build -j2 | Passed; static and shared library builds |
| ctest --test-dir build --output-on-failure -j2 | 936 total: 924 passed, 12 existing UDP diagnostic skips, zero failures |
| ASan targeted callback tests | 18 passed, zero failures; leak detection enabled |
| clang-format, cmake-format and git diff --check | Passed |

The ASan build enables both WIRESTEAD_ENABLE_SANITIZERS and
WIRESTEAD_ENABLE_ASAN. The test executable links libasan.so.8 and ran with
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1. Both new test executables were
rebuilt after the UDS forwarding fix before the successful run.

Local logs are under the workspace's log/codex-d2-validation directory:
build-final.log, full-ctest-final.log, asan-build-final.log and
asan-tests-final.log. Baseline reproduction logs are kept there separately.

## Limits

The tests use controlled channels and local loopback sockets on Linux/WSL.
They do not claim a complete Cartesian product of source and destination
transport types or independent local Windows/macOS validation. Cross-channel
behavior is covered using same-type targets; thread-local isolation has its
own test. CI provides the platform build/test matrix.

D-3 structured send results, the undecided raw executor-work rule C-5.4-5,
and custom transport/framer extension semantics are outside this change.
No public API signature or try-send policy changes.

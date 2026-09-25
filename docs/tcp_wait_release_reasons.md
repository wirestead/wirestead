# TCP capacity wait release reasons

> Historical implementation-stage record. Public return types and custom-channel
> compatibility have since changed; see [the current client API](client_send_results.md).

Built-in TCP connections now own a small wait record. A pinned sender keeps the
record alive across connection replacement or wrapper restart. The submission
mutex protects both first-cause publication and polling of the record.

An explicit wrapper stop publishes cancellation before changing its started
flag and notifying waiters. A direct transport stop publishes the same cause.
Connection loss publishes NotReady. Later terminal events never replace the
first cause, including the disconnect performed by stop cleanup itself.

## Wait-stage SendResult

Once a call needs to wait for capacity, its internal result is:

- CancelledWhileWaiting if stop ended the pinned connection first.
- NotReady if connection loss ended it first, even if stop or reconnect follows.
- An accepted stage result when polling observes usable capacity under the
  submission mutex. The result is cached and later stop cannot rewrite it.
- WouldBlock if the calling context is a user callback that cannot wait.

An accepted wait-stage result is not transport admission. The final wrapper
run and connection checks and the conditional transport admission from PR #669
still apply. For example, a stop after capacity was selected can make the
public bool send fail while the recorded wait-stage outcome remains accepted.

Entry paths that do not wait continue to bypass this stage and use the existing
final admission checks. Public sends still return bool; this does not expose
new result-returning send APIs or complete D-3 state/capacity classification.

## Regression evidence

Tests observe the internal result only through a null-by-default scheduling
hook. Six send forms cover explicit stop/restart and reconnection, plus:

- Connection loss followed by stop: NotReady remains.
- Stop followed by cleanup/disconnect: CancelledWhileWaiting remains.
- Actual socket capacity release selected before stop: the stage result stays
  accepted and final admission still rejects the stopped send.

There are 54 capacity-wait cases in total, including 18 new ordering/release
cases. Temporarily allowing terminal reasons to overwrite one another makes
all 12 stop/loss ordering cases fail; first-cause preservation was restored
before final validation.

## Validation

- Full Debug build and CTest with -j2: 1083 discovered, 1071 passed,
  12 existing UDP diagnostic skips, zero failures.
- After strengthening direct-stop coverage and adding explicit assertion
  braces, all 54 capacity-wait cases passed again.
- AddressSanitizer with leak detection: 118 selected TCP tests passed; the
  final 54 capacity-wait cases also passed after the test refinement.
- TLS-enabled build: 86 selected TCP/TLS tests passed.
- clang-format and git diff --check passed.

The new scheduling cases use plaintext sockets. Existing TLS tests cover
encrypted paths, not exhaustive TLS cancellation scheduling.

## Server statistics during disconnect

Server statistics read retained totals and live sessions under the same session
mutex used to transfer counters when a session is removed. Taking the retained
snapshot before acquiring that lock allowed a concurrent disconnect to remove
the session after the old totals were read, transiently omitting its counters.
TCP and UDS use the same corrected lock ordering. Public statistics fields and
reset semantics are unchanged.

Validation after this correction: full Debug CTest passed (1071 passed,
12 existing skips); the four TCP/UDS cumulative and per-session statistics
tests each passed 30 repetitions; ASan with leak detection passed all 28
TCP/UDS server lifecycle cases. The existing counter-retention assertions
remain unchanged.

## Scope and remaining work

The stable terminal record applies to built-in TCP transports, including an
injected built-in TcpClient. Custom Channel implementations keep their existing
snapshot-based fallback; they do not gain transport-level event ordering.

Public SendResult returns, complete initial/admission rejection classification,
other transports, custom channels, fanout results and bindings remain D-3 work.
Public method signatures, Channel vtables and object layouts are unchanged.

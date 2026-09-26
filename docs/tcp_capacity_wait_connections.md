# TCP capacity waits across reconnect

> Historical implementation-stage record. Public return types and custom-channel
> compatibility have since changed; see [the current client API](client_send_results.md).

Blocking/Reliable wrapper sends using the built-in TCP transport now capture
both the wrapper run and the usable TCP connection at entry. The connection
snapshot is taken under the transport submission mutex and stays fixed across
all capacity waits and unbounded capacity retries.

A changed or unavailable connection ends the wait, including when the new
connection is ready and pressured again. Copy, move and shared admission then
check the expected connection under the same mutex that protects readiness,
capacity reservation and strand submission. This closes the race between the
wrapper's final check and the transport's acceptance decision.

An old send returns false without accepting its payload on the replacement
connection. A new send can target the replacement normally. The guarantee also
covers send/send_line and their explicit blocking variants.

## Compatibility boundary

This uses private access between the matching built-in wrapper and transport.
Public Channel methods and vtables, object layouts and existing write
signatures are unchanged. Public sends still return bool.

An injected built-in transport::TcpClient receives the same guarantee.
Arbitrary injected Channel implementations retain their existing behavior and
the stop/restart run protection from PR #668; they do not acquire a connection
identity through this change. Their connection/admission synchronization
remains the custom implementation's responsibility.

## Regression evidence

Eighteen real loopback cases cover six send forms at three boundaries:

- Waiting while the old connection ends and the replacement has free capacity.
- Waiting while the replacement connection is pressured again.
- After the wrapper's final check, immediately before the transport takes its
  admission mutex, while the connection is replaced.

The first twelve failed before the fix. All eighteen pass afterwards. As a
negative control, temporarily removing only the transport's expected-connection
check makes all six final-admission cases fail. That check was restored before
the final validation.

Tests keep a real socket write under pressure using a small peer receive
buffer, pause at internal scheduling seams, reconnect without stopping the
wrapper, and verify rejection and unchanged acceptance counters. Fresh sends
on the replacement still succeed.

## Validation

- Full Debug build and CTest with -j2: 1065 discovered, 1053 passed,
  12 existing UDP diagnostic skips, zero failures.
- AddressSanitizer with leak detection: 100 selected TCP tests passed.
- TLS-enabled build: 68 selected TCP/TLS tests passed.
- clang-format and git diff --check passed.

The new reconnect scheduling cases use plaintext sockets. Existing TLS tests
exercise encrypted paths; exhaustive TLS reconnect scheduling is not claimed.

## Remaining work

The [wait-reason follow-up](tcp_wait_release_reasons.md) retains a structured,
stable CancelledWhileWaiting/NotReady result internally. Public sends still
return bool. Public SendResult integration and
equivalent handling for other transports, custom channels, fanout and bindings
remain D-3 work.

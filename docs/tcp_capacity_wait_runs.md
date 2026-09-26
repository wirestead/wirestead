# TCP capacity waits across stop/restart

> Historical implementation-stage record. Public return types and custom-channel
> compatibility have since changed; see [the current client API](client_send_results.md).

TCP blocking and Reliable sends capture the wrapper run generation once at
entry. They carry it through capacity waits and all admission retries.
The wait predicate exits when that generation changes, even if a restarted
channel is connected and pressured again. The final generation check and
transport call run under the wrapper lock also used by stop/start.

This prevents a stopped sender from accepting data into a replacement run.
send, send_line, send_blocking, send_line_blocking, send_move and send_shared
all follow this rule where they can wait. They continue to return bool: an old
run returns false, while a fresh send can succeed after restart.

## Regression coverage

An injected channel holds pressure active while a scheduling seam pauses the
sender immediately before registering its timed wait. The test performs stop
and optionally restart before releasing the sender, covering a missed stop
notification and an immediate restart deterministically.

Eighteen cases exercise six send forms with stop only, restart with free
capacity, and restart under pressure. The twelve restart cases failed before
the fix: they either accepted into the new run or waited for its pressure.
Stop-only cases retain their existing rejection behavior. Fresh sends after
restart remain accepted.

The generation stays fixed across retries rather than being sampled anew for
each attempt. No new public methods, return types or ABI changes are introduced.

## Validation

- Debug build and full CTest with -j2: 1047 discovered, 1035 passed,
  12 existing UDP diagnostic skips, zero failures.
- AddressSanitizer with leak detection: all 45 selected TCP tests passed,
  including all 18 new scheduling cases.
- clang-format and git diff --check passed.

## Remaining work

A wrapper run is not a TCP connection instance. Automatic disconnect/reconnect
without an explicit stop is handled for the built-in transport by the
[connection-wait follow-up](tcp_capacity_wait_connections.md), including its
final transport admission check. Structured, stable
CancelledWhileWaiting/NotReady results are not implemented by this bool fix.
Other transports, fanout results and bindings remain separate D-3 work.

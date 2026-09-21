# UDP server blocking admission

PR #661 exposed a remaining inconsistency: UDP server blocking sends waited
for capacity, then called async_try_write_to. That nonblocking path applies
the lower pressure watermark, so a request above that watermark could never
be accepted even after the queue drained.

## Change

After the existing wait, send_to_blocking looks up the destination again
under the wrapper mutex, checks the server is started, and submits through
async_write_to. The transport still checks shutdown state, payload validity
and the hard queue limit. Bounded retries and callback no-wait rules remain.

This covers Reliable send_to, Reliable send_to_line, and explicit
send_to_blocking under either strategy. Explicit try-send and ordinary
BestEffort send_to/send_to_line retain async_try_write_to. Broadcast is
unchanged. Public signatures and ABI do not change in this patch.

## Verification

Three new tests exercise a 1 KiB pressure watermark with a 2 KiB payload
that fits both the hard queue cap and a UDP datagram:

- Reliable raw/newline sends and explicit blocking sends deliver the exact
  payload to a real loopback peer when there is no pressure.
- The same APIs wait while pressure is held, then accept and deliver after
  pressure clears. Explicit blocking is also checked under BestEffort.
- Nonblocking calls keep refusing that payload under both strategies, and
  ordinary BestEffort sends keep refusing, with and without active pressure.

Before the fix the two delivery tests failed every API case; the
nonblocking control passed. The existing UDP exact-hard-limit test now
expects acceptance after waiting, matching TCP and UDS. That large UDP
boundary test checks acceptance only: it is not evidence that an oversized
datagram can be delivered.

- Full Debug build with -j2 passed.
- Full CTest: 979 discovered, 967 passed, 12 existing UDP diagnostic skips,
  zero failures.
- All 43 related tests passed under AddressSanitizer with
  ASAN_OPTIONS=detect_leaks=1:halt_on_error=1.
- Repository clang-format and git diff --check passed.

## Remaining work

Structured acceptance results, connection-instance fencing, stable
cancellation reasons and fanout aggregates remain D-3 work. UDP Reliable
still means local queue admission, not network delivery reliability.

# Readiness before capacity waiting

This is a scoped prerequisite for the state stage of the
[v0.10 acceptance procedure](communication_contract_v0.10_decisions.md).
Payload-size validation landed separately in PR #658.

## Behavior

A TCP client that was started but is not connected must reject a valid
blocking send without waiting for stale queue pressure to clear. The same
check releases a capacity waiter when readiness is lost and remains lost.
TCP now checks readiness in its wait predicate, as UDS, UDP and serial
already do, and checks it again before handing a copied payload to the
transport. Move and shared sends already had that second check.

A UDP server shares channel pressure across its sessions. A blocking send to
an unknown client ID must not wait on that shared pressure. Its predicate now
checks session membership while holding the existing wrapper mutex. The
transport submission still performs its existing target lookup. TCP and UDS
servers already report no pressure for an absent session.

The periodic 50 ms predicate check remains, so a missed or absent capacity
notification cannot leave these callers waiting indefinitely. This is not a
hard real-time response deadline.

## Regression coverage

The existing validation-before-wait test fixture now includes 11 additional
tests:

- Each of the four client wrappers is tested while not ready, with pressure
  still active, across six blocking-capable send forms.
- Each client is also tested after a valid call has begun waiting: readiness
  is removed without a backpressure notification and the call must reject.
- Each of the three server wrappers sends to an absent ID while a real
  session is under pressure, through Reliable send_to, send_to_blocking and
  send_to_line.
- Existing valid-input controls continue to prove normal waiting and
  acceptance after pressure clears.

Before the patch, the two TCP readiness tests and UDP absent-target test
failed their two-second bounds; the other eight tests passed. The isolated
fixed draft passed all 25 tests including the previous 14 payload tests.

## Local validation

- Full Debug build: cmake --build build -j2 passed.
- Full CTest: 961 discovered, 949 passed, 12 existing UDP diagnostic skips,
  zero failures (ctest --test-dir build --output-on-failure -j2).
- All 25 validation-before-wait tests passed with AddressSanitizer and
  ASAN_OPTIONS=detect_leaks=1:halt_on_error=1.
- Repository clang-format and git diff --check passed.

## Scope limits

No public signatures or return types change. This does not pin a connection
instance across a wait, detect a disconnect/reconnect between observations,
or stabilize cancellation reasons across stop/restart races. Whole-queue
limits and structured results remain outstanding. The TCP copied-send path
now rejects at the wrapper when not ready, matching its move/shared paths
and the other clients; it no longer invokes transport failure accounting for
that wrapper-level refusal.

# Whole-queue hard limits before waiting

This follows payload-size validation in PR #658 and readiness fixes in
PR #659. It addresses the whole-queue hard-limit part of C-3.1-1b before the
structured D-3 result conversion.

## Behavior

A payload can be below MAX_BUFFER_SIZE but larger than the entire configured
write queue. Such a payload cannot become admissible by draining the queue.
Reliable and explicit blocking calls now bypass capacity waiting for that
case across all seven wrappers. Newline-appending calls compare the complete
payload, including the delimiter.

The existing transport write still performs rejection. The wrapper does not
fabricate a result or take over transport error/accounting behavior. Valid
sizes still wait; try-send and BestEffort submission paths are unchanged.

## Limit reporting

Channel::write_queue_limit() reports an optional byte count: the whole
write queue's hard limit, not its currently free capacity. The default is
nullopt, meaning unavailable. Existing custom Channel subclasses compile
without an override and retain their previous waiting behavior; implementers
must report the same limit their write admission enforces.

TCP, UDS, UDP and serial clients report their actual stored transport cap.
TCP and UDS servers expose write_queue_limit(ClientId) for a session, with
nullopt for an absent ID, and read it under the existing session-map mutex.
Their parameterless Channel query remains unavailable because sessions have
separate queues. UDP server queries its shared UDP channel.

The built-in hard cap is stable during a transport run. This query is not a
capacity reservation or an acceptance guarantee. A reported cap can exceed
the per-message maximum; both constraints still apply.

## Compatibility

The added virtual Channel method changes the binary interface. Rebuild the
library, custom channel implementations and consumers together for v0.10.
Existing source implementations remain compatible through the default
method. Public send signatures and bool results are unchanged.

## Verification

Fifteen new tests cover all seven wrappers: over-limit payloads bypass the
wait, while exact-limit payloads retain waiting. Client tests cover copy,
move, shared and both newline forms, using a fake channel with a limit
different from wrapper configuration. Real server loopbacks hold pressure
while testing sends, explicit blocking sends and newline sends. Concrete
client transports report expected caps for two configured thresholds.
Existing tests with unavailable metadata retain their previous behavior.

Before the wrapper changes, all seven over-limit tests exceeded their
two-second bounds. The initial UDP exact-limit test exposed an existing
stricter submission policy, described below; its expectation now checks that
policy rather than changing it.

- Full Debug build with -j2 passed.
- Full CTest: 976 discovered, 964 passed, 12 existing UDP diagnostic skips,
  zero failures.
- All 40 related tests passed with AddressSanitizer and leak detection.
- scripts/verify_installed_consumer.sh --library-mode both passed, using
  CMAKE_BUILD_PARALLEL_LEVEL=2 and a fresh Debug build/install.
- Repository clang-format and git diff --check passed.

## Later UDP admission update

The subsequent [UDP server admission fix](udp_server_reliable_admission.md)
replaces the blocking path's try-write submission with ordinary write
admission. Its exact-hard-limit test now expects acceptance on UDP as well.
The paragraph below records the limitation when PR #661 landed.

## Scope limits at PR #661

UDP server's blocking send still calls async_try_write_to, which rejects a
request above its lower pressure threshold even when it fits under the whole
queue hard cap. The exact-hard-limit test proves waiting is preserved and
then expects that existing refusal. This patch does not unify those two
thresholds or claim every potentially impossible send is handled.

Connection-instance fencing, synchronized acceptance, stable waiter
cancellation reasons, fanout aggregates and structured results remain D-3
work. Runtime statistics semantics and retry counts are unchanged.

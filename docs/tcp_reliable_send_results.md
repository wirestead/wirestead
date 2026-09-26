# TCP Reliable send results

> Historical implementation-stage record. Public return types and custom-channel
> compatibility have since changed; see [the current client API](client_send_results.md).

Built-in TCP send/send_line/send_move/send_shared under Reliable and explicit
send_blocking/send_line_blocking now carry an internal SendResult across their
whole decision path. Public signatures still return bool.

## Decision path

Payload shape, maximum size and the available native hard queue limit are
validated before state or waiting. Line delimiters count toward the payload.
Wrapper lifecycle and native readiness are checked before pinning the connection
and run. Invalid input bypasses native handoff and native failure/drop counters,
matching the nonblocking behavior introduced in PR #673. This also rejects
hard-limit-oversize input on explicit blocking calls under BestEffort before it
can be accepted for a later native drop.

A capacity wait returns its retained outcome first. CancelledWhileWaiting from
stop and NotReady from loss are returned unchanged, even when the wrapper has
already stopped or restarted. An accepted wait-stage result only permits a
final check; it is not yet transport acceptance.

After capacity release, wrapper lifecycle and run identity are checked again.
Native admission checks the pinned connection, native state and reservation
under its submission mutex. A successful admission is acceptance only, never a
delivery receipt. Later connection loss remains a statistics concern.

Only native WouldBlock is retried, without an attempt limit, using the original run
and connection pin. Other rejections return immediately. A callback caller
returns WouldBlock without a capacity wait or another retry. Exhausted capacity
retries also return WouldBlock, not the BestEffort-specific QueueFull.

An immediate callback send can still succeed when capacity is available.
Explicit blocking methods preserve their ordinary native admission policy even
when the configured strategy is BestEffort.

## Custom channels and public API

Custom Channel implementations retain bool-based delegation, validation,
accounting and unbounded capacity retries. Their bool refusal is not assigned a made-up
typed result. Public SendResult API migration, other transports, custom channel
result support, fanout and bindings remain separate work.

## Regression coverage

The real TCP wait/reconnect matrix now observes both the wait-stage and final
send outcome for six send forms. It covers replacement connections, loss before
stop, stop before cleanup, capacity selected before stop, successful acceptance
after a capacity wait, and callback pressure refusal.

Eight entry cases cover Reliable and explicit blocking under BestEffort,
validation precedence, lifecycle and retained move storage. A deterministic
inflight-reservation test fills the hard cap without advancing the executor:
copy, move and shared sends stop after five capacity refusals, preserve caller
storage on rejection, and emit one final wrapper outcome each. Existing custom
channel validation/wait tests remain in place.

## Validation

- Full Debug build and CTest with -j2: 1166 discovered, 1154 passed,
  12 existing UDP diagnostic skips, zero failures.
- ASan with leak detection: 220 selected TCP transport and wrapper cases passed.
- TLS-enabled build: 121 selected TCP/TLS cases passed.
- clang-format and git diff --check passed.

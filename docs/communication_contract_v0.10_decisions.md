# v0.10 Contract Decisions

Decision proposals for the common differences in
[the audit](communication_contract_v0.10_audit.md) section 9.1.
**Implementation status:** D-1 landed for TCP in PR #652, UDS in PR #654,
UDP in PR #655 and serial in PR #656. D-2 landed in PR #657, and payload-size
validation before waiting landed in PR #658. PR #659 repaired TCP
readiness and UDP server target checks before capacity waiting. This follow-up
adds reported whole-queue hard limits to validation before waiting. Structured
results and connection-instance fencing remain separate work.

The order is by dependency, not by impact. D-1 defines when a shutdown is
complete, D-2 needs a rejection that D-3 then gives a name to.

| | |
| --- | --- |
| Baseline for the observations quoted | `d914b8d7c`, plus the serial fix in #650 |
| Contract | [communication_contract_v0.10.md](communication_contract_v0.10.md) |
| Already fixed under a narrower contract | serial `stop()` from inside a callback (#650); D-1 generalizes it |

Each decision states the guarantee, what it applies to, its exceptions and
caller preconditions, its compatibility impact, and how it is verified. A
decision is not "done" because one target satisfies it: every target needs its
own evidence, as in the audit.

## D-1: concurrent and callback-initiated `stop()` (C-5.4-2, C-5.4-3)

### Guarantee

Two states are distinguished, and the contract names both:

- **Shutdown requested** - a stop has begun; no new work is accepted.
- **Shutdown complete** - exactly as contract section 1 defines it, including
  its allowance that outstanding internal work may remain when it holds its own
  lifetime independently of the object. This decision does not restate or
  strengthen that definition.

Then:

1. `stop()` called from a thread that the target's shutdown does not need in
   order to make progress returns only when shutdown is complete. This holds
   for every concurrent caller, and for a caller that finds a shutdown already
   requested or already in progress: it waits rather than returning early.
2. `stop()` called from a thread the shutdown **does** need - the target's own
   callback, or any other work on the executor that has to run for the
   shutdown to finish - requests the shutdown and returns without waiting. It
   never waits for itself.
3. `stop()` after shutdown is complete returns immediately and changes nothing.
4. After a `stop()` that waited returns, `start()` is allowed.

**Scope of rule 2, decided here:** the criterion stays the contract's
(section 5.4) - whether the target's shutdown needs the executor this thread is
running - rather than "inside any callback". A callback of an unrelated channel
on an unrelated executor therefore still gets the waiting form, and does not
silently lose the completion guarantee. The implementation must be able to
answer that question for the target, for example by testing the target
executor's `running_in_this_thread()`; a thread-local "inside some callback"
flag answers a different question and would widen rule 2 beyond this decision.

**Destruction is not included in rule 4.** It keeps the contract's own
precondition (section 5.4): an object is destroyed only once its shutdown is
complete and nothing else is accessing it concurrently, and a waiting shutdown
is never performed on an executor thread the shutdown needs. In particular,
destroying an object from inside its own callback is not supported, and a
callback-initiated `stop()` does not make it so: it requests a shutdown, and
something outside still has to observe completion before destruction.

### Applies to

Every target's public `stop()`: TCP client, TCP server, UDS client, UDS server,
UDP, UDP server, serial - and the transport `stop()` beneath each. Destructors
that stop as part of destruction inherit the same rules, but the destruction
precondition above is what decides where a destructor may run at all.

### Exceptions and caller preconditions

- Rule 1 cannot outrun a user callback that never returns; the wait is bounded
  by the callbacks the caller's own code supplies.
- With an externally run io_context, the caller keeps that executor running
  while waiting, and does not call the waiting form on one of its threads.
- `start()` concurrent with `stop()` stays a caller precondition (contract
  5.5): this decision does not make them safe to overlap.
- Restarting from inside a callback, before shutdown completes, is not
  supported.

### Compatibility impact

- A second `stop()` that returns immediately today will block until the first
  completes. Code that calls `stop()` from two threads and expects the second
  to be cheap changes behavior; nothing breaks at compile time.
- Rule 2 changes a self-join into a request on the targets that still join
  unconditionally. Serial was the only one found doing that, and is fixed
  (#650); the audit rows for the other six say they already skip the join, but
  each still needs its own test.
- No signature changes, so no ABI impact beyond the behavior.

### What completion is evidenced by

The definition in section 1 of the draft applies to both owned and external
executors. Keeping queued work alive is necessary but not sufficient: that
work must also be isolated from the next run.

For the TCP implementation in PR #652, outside callers wait for the transport
cleanup signal and for the wrapper callback gate to become idle. Client completion includes cancelled I/O handler exit; server cleanup
includes completion on every live session's strand. An owned thread is then
joined. A caller executing the target's io_context requests cleanup without
waiting; a subsequent outside stop observes its completion. A never-started
transport has no run to drain and can finish cleanup inline.

A stopping thread never polls the caller's executor or infers completion from
a stopped context or a timeout. External executors must keep progressing as
required above. Outstanding cancellation/retry handlers retain their own
lifetime and are rejected by their run generation or the closed session;
this does not permit deferred cleanup to mutate a restarted transport.

The UDS follow-up uses the same completion boundary. Its client also orders
accepted write submissions before the cleanup request, and retains I/O
completion ownership until cancellation handlers finish. Its server collects
completion from every session strand before releasing its socket-path
ownership. UDS interface callbacks explicitly dispatch onto the owning strand:
passing a bound handler through `std::function` alone does not preserve its
associated executor. Synchronous startup validation/bind failures have no run
to drain and can complete shutdown without starting an executor.

See [UDS validation](uds_d1_validation.md) for target-specific evidence.

### Verification

Per target, not once:

1. Two outside threads call `stop()` concurrently while a slow callback is
   running; assert both return only after the callback has finished and that no
   callback runs after either return.
2. The combined path, which testing each rule separately does not cover: a
   callback requests `stop()` and then keeps running, while two outside threads
   call `stop()`. Neither outside caller returns until the callback has been
   left and shutdown is complete; the callback's own `stop()` returns
   immediately.
3. `stop()` from inside each callback kind returns within a bound and does not
   throw.
4. After a waiting `stop()` returns, a restart succeeds **and the restarted
   channel receives data** (the shape `SerialStopInCallbackTest` already uses).
5. A third `stop()` after completion returns immediately.
6. For the externally-run-io_context configuration, the same as 1 with the
   executor running.

## D-2: blocking sends inside any callback (C-5.4-4)

### Guarantee

While the calling thread is executing **any** user callback the library
invoked, a blocking send never waits. If it would have to wait for queue
capacity, it is rejected immediately. If no wait is needed, the ordinary
acceptance procedure applies unchanged - every other rejection condition still
holds. Being inside a callback is not itself a reason to refuse: a send from
`on_disconnect` or `on_error` is refused when the target is not ready to send,
and a send to a different, healthy channel from the same callback is accepted
normally.

"Any callback" is the whole set, not the data path only: `on_data`,
`on_message`, their batch forms, `on_connect`, `on_disconnect`, `on_error`,
`on_backpressure`, and any callback added later. It also applies when the
callback belongs to a different channel that shares the executor, because that
thread cannot make progress for either.

### Applies to

Every blocking-capable send on every target: `send()`/`send_line()` under the
Reliable strategy, `send_blocking()`/`send_line_blocking()`, `send_move()` and
`send_shared()` under Reliable, and the servers' `send_to()` under Reliable and
`send_to_blocking()`. `try_send*()` is unaffected - it already never waits.

### Exceptions and caller preconditions

- A blocking send from an executor thread that is **not** inside a callback is
  a separate rule (C-5.4-5) and is not decided here.
- The rejection is immediate, so a caller inside a callback handles a refusal
  rather than assuming delivery; the documented way to send from a callback
  stays `try_send*()`.
- What the rejection *says* comes from D-3. Until D-3 lands it is `false`.

### Compatibility impact

- Today a blocking send from `on_connect`, `on_disconnect`, `on_error` or
  `on_backpressure` can wait on the thread that would have to clear the
  backpressure. Afterwards it returns a rejection. Code that relied on it
  blocking there changes behavior; code that already handles a `false` return
  does not.
- The data and message callbacks already behave this way, so the common case is
  unchanged.
- Implementation: every listed callback path in the seven wrappers reaches
  detail::invoke_user_callback, including timer batch flushes. That common
  invocation now holds a depth-counted guard during the user call and restores
  it before exception logging. Existing receive-path scopes remain in place
  to preserve their prior extent; nesting does not clear an outer guard.
  D-1 callback admission and target-executor shutdown detection are unchanged.
  See [D-2 validation](callback_d2_validation.md) for evidence and limits.

### Verification

For each target, for each callback kind, and for each blocking send API:

1. With backpressure active, a blocking send from that callback returns within
   a bound instead of waiting, with the rejection D-3 defines.
2. With capacity available, the same call from the same callback is accepted
   when the other acceptance conditions hold, and otherwise rejected with the
   reason those conditions give - not with a callback-specific refusal. A send
   to a different, ready channel from inside a callback is accepted.
3. A blocking send from an outside thread still waits as before - the rule does
   not leak out of callbacks.

## D-3: structured acceptance result (C-3.7-1)

### Guarantee

Every send API returns a structured result that reports **acceptance only**:
the request was accepted, or it was rejected for one stated reason. The result
never describes anything that happens after acceptance.

Shape (names to settle in review):

```cpp
enum class SendRejection {
  NotStarted, Stopping, NotReady, WouldBlock,
  QueueFull, TooLarge, InvalidArgument, CancelledWhileWaiting
};

class SendResult {
 public:
  bool accepted() const;
  SendRejection reason() const;         // only meaningful when !accepted()
  explicit operator bool() const;       // accepted()
};
```

Which situation yields which reason is part of this decision, not of the
implementation:

| Situation | Reason |
| --- | --- |
| Never started, or stopped and not started again | `NotStarted` |
| A shutdown has been requested and not completed | `Stopping` |
| Started, but the target is not ready to send (contract section 2 defines readiness per transport; this replaces the transport-specific `NotConnected` spelling) | `NotReady` |
| The connection instance a sender waited on ended, even if a new one is ready (contract 3.1) | `NotReady`, and the request is never accepted onto the new connection |
| `stop()` while a sender was waiting for capacity | `CancelledWhileWaiting` |
| Capacity is short where waiting is not permitted: `try_send*()`, or a blocking send under D-2 | `WouldBlock` |
| Capacity is short and the BestEffort strategy refuses the new request | `QueueFull` |
| Payload empty, null, or above the per-message maximum | `InvalidArgument` for shape, `TooLarge` for size |

When several apply at once, which one is reported depends on where the call
is, and the two points are ordered separately.

**On first entry**, the contract's own decision procedure decides:
`InvalidArgument`/`TooLarge` (stage 1 validation), then
`NotStarted`/`Stopping`/`NotReady` (state), then `WouldBlock` (call site), then
`QueueFull` (strategy).

**When a wait ends**, the reason the wait ended is decided **first**, and a
later state never overwrites it:

| The wait ended because | Reason |
| --- | --- |
| `stop()` | `CancelledWhileWaiting` |
| The connection instance it waited on ended | `NotReady` |
| Capacity became available | No rejection yet - continue below |

Only in the third case does the call re-check state, connection instance and
capacity, as one synchronized decision (contract 3.1), and report whatever that
re-check yields. Without this split, a sender woken by `stop()` would be read
as `Stopping` on the state check and the mapping table above would contradict
itself.

**When `stop()` and a connection loss race**, the cause is fixed at the same
synchronized point that releases the waiter: whichever cause that point
observes is the one reported, and the other, arriving afterwards, does not
change it. This makes the reported reason stable rather than dependent on how
long the woken thread took to be scheduled.

Three reporting paths stay separate, and this decision keeps them apart:

| What | Reported through |
| --- | --- |
| Acceptance or immediate rejection of one send | `SendResult` |
| Partial acceptance across sessions (fanout) | A separate aggregate result, decided with C-3.6-3/4 |
| Loss of a request **after** acceptance | Statistics only, decided with C-3.8-1 |

The third cannot be folded into `SendResult`: the call has already returned by
the time such a loss happens.

### Applies to

The 40 public send entry points across `ichannel.hpp`, `iserver.hpp` and the
seven wrappers, and the four transport-level write entry points behind them.
Fanout APIs (`broadcast()`, `try_broadcast()`) return the aggregate type
instead, so their decision has to land with this one or immediately after.

### Exceptions and caller preconditions

- `reason()` is only defined when the result is not accepted.
- An accepted result is not a delivery receipt; the contract's acceptance
  definition is unchanged.
- The enum is expected to grow. Callers switch on it with a default branch.

### Compatibility impact

This is the largest break in the three, and it belongs in v0.10 rather than
later.

- **Recommended transition: replace the return type in place**, with an
  `explicit operator bool`. `if (port->send(x))` and `if (!port->send(x))`
  keep compiling; `bool ok = port->send(x);` does not, and has to be changed to
  `port->send(x).accepted()` or an explicit conversion.
- The alternative, keeping `bool` and adding parallel `*_ex()` APIs, doubles
  the surface permanently and leaves the reason invisible by default. Not
  recommended.
- Downstream, at the time of writing: `wirestead-python` exposes
  `send(...) -> bool` on four classes, so the binding needs a mapping decision
  (truthiness plus an accessor, or a Python enum) and its stubs regenerate.
  `wirestead_ros` does not call `send()` and is unaffected.
  `wirestead-examples` and every documented snippet use the `if (send(...))`
  form and keep compiling, which `scripts/check_docs_compile.sh` checks.
- ABI breaks, which v0.10 already does.

### Verification

1. One test per rejection reason, asserting the reason rather than only the
   refusal.
2. The situations that apply to every target - not ready, stopping, waiting
   cancelled by `stop()`, capacity short - are checked **on each target** to
   map to the same reason. One target per reason would leave the common
   mapping unverified, which is what the audit found for other rules.
3. Every target keeps its existing accept/reject tests, now reading
   `accepted()`.
4. `scripts/check_docs_compile.sh` passes, which is what proves the documented
   `if (send(...))` form still compiles.
5. The installed-consumer smoke builds against the new headers.
6. The Python binding's own tests, once its mapping is decided.

## Dependencies of the remaining section 9.1 rows

| Row | Depends on | Note |
| --- | --- | --- |
| C-3.1-1b validation before waiting | none for the fix; D-3 for the reason | Validating before the wait is implementable with today's `bool`. D-3 only makes the resulting rejection distinguishable from a queue refusal |
| C-3.2-3 keep-latest on the blocking path | Contract open item (keep-latest policy), then D-3 | Not decidable until keep-latest is either adopted as an opt-in policy or dropped from v0.10 |
| C-3.6-3, C-3.6-4 fanout result | D-3 | Needs the aggregate type defined alongside `SendResult`; zero targets must stay distinguishable |
| C-6.1-1 queued data across a link loss | C-3.8-1 for observability; event model for the notification | Discarding is already **Decided** in contract 6.1 - "data from a previous connection is never sent on a new one" - so what is left is implementing the discard and deciding how it is observed and announced, not whether it happens |
| C-6.1-2 reason a blocked sender was released | D-1 and D-3 | D-1 says who releases the waiter, D-3 carries the reason out |

## Suggested build order

1. D-1, per target, reusing the shape proven on serial.
2. D-2: first establish which callback paths reach the shared dispatch and
   whether the guard is restored when a callback leaves through an exception or
   nests, then widen the refusal to the paths that check out, with tests per
   callback kind.
3. D-3, then the fanout aggregate (C-3.6-3/4) immediately after, since callers
   should meet both in the same release.

Each lands as its own change with its own tests; none of them waits on the
still-open C-5.4-3, C-1-1 or event-model decisions.

## Validation-before-wait follow-up

The empty-payload and per-message maximum portions of C-3.1-1b are implemented
across all seven wrappers before introducing D-3's result type. Invalid sizes
bypass the capacity wait and reach the same transport validation path, keeping
its refusal, error callbacks and accounting. Line payloads are judged after
their newline is appended; an empty line remains a valid one-byte request.

This does not claim the entire stage-1 decision is implemented. Detecting a
payload larger than a transport's configured whole-queue limit, readiness
before waiting, connection-instance fencing and structured reasons remain
part of the subsequent D-3 work. See
[validation evidence](send_validation_before_wait.md).

## Readiness-before-wait follow-up

TCP now observes disconnected state in its capacity predicate, matching the
other three client wrappers, and checks readiness again before submitting a
copied payload. UDP server waiting now observes whether the target client ID
still exists, matching the absent-session behavior of TCP and UDS servers.

This covers a target that is unavailable on entry or stays unavailable when
the waiter next checks it. A disconnect/reconnect entirely between checks,
restart generation fencing and stable cancellation reasons are still D-3
work. See [readiness validation](send_readiness_before_wait.md).

## Whole-queue-limit follow-up

Blocking-capable sends on all seven wrappers bypass capacity waiting when the
payload is larger than the concrete transport's reported whole-queue hard
limit. The transport retains rejection and accounting. Custom channels may
report no limit; their existing behavior is preserved until they implement
the optional query.

This does not change UDP server's existing try-write submission path, which
also applies its lower pressure threshold. Nor does it complete the
synchronized state/connection decision or structured results. See
[queue-limit validation](send_queue_limit_before_wait.md).

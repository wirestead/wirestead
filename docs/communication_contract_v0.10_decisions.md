# v0.10 Contract Decisions

Decision proposals for the common differences in
[the audit](communication_contract_v0.10_audit.md) section 9.1. **Status:
proposed, pending approval.** Nothing here is implemented yet, and approving a
decision does not schedule its implementation: each is meant to be built on its
own.

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
- **Shutdown complete** - as defined in contract section 1: no user callback of
  the object is running, none from that run will start, outstanding internal
  work cannot touch the object, and nothing from the run survives into a later
  one.

Then:

1. `stop()` called from **outside** the library's execution threads returns
   only when shutdown is complete. This holds for every concurrent caller, and
   for a caller that finds a shutdown already requested or already in progress:
   it waits rather than returning early.
2. `stop()` called from **inside a user callback**, or on an executor thread
   whose progress the shutdown needs, requests the shutdown and returns without
   waiting. It never waits for itself.
3. `stop()` after shutdown is complete returns immediately and changes nothing.
4. After an outside `stop()` returns, `start()` and destruction are both
   allowed.

### Applies to

Every target's public `stop()`: TCP client, TCP server, UDS client, UDS server,
UDP, UDP server, serial - and the transport `stop()` beneath each. Destruction
follows rule 1 when it runs outside the executor, and rule 2 when it does not.

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

### Verification

Per target, not once:

1. Two outside threads call `stop()` concurrently while a slow callback is
   running; assert both return only after the callback has finished and that no
   callback runs after either return.
2. `stop()` from inside each callback kind returns within a bound and does not
   throw.
3. After an outside `stop()` returns, a restart succeeds **and the restarted
   channel receives data** (the shape `SerialStopInCallbackTest` already uses).
4. A third `stop()` after completion returns immediately.
5. For the externally-run-io_context configuration, the same as 1 with the
   executor running.

## D-2: blocking sends inside any callback (C-5.4-4)

### Guarantee

While the calling thread is executing **any** user callback the library
invoked, a blocking send never waits. If it would have to wait for queue
capacity, it is rejected immediately; if capacity is available, it is accepted
as usual.

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
- Implementation note, not part of the contract: the existing guard is set at
  each dispatch site, which is why four callback kinds are uncovered. Setting
  it once inside the shared user-callback invocation would cover every kind and
  every future callback.

### Verification

For each target, for each callback kind, and for each blocking send API:

1. With backpressure active, a blocking send from that callback returns within
   a bound instead of waiting, with the rejection D-3 defines.
2. With capacity available, the same call from the same callback is accepted.
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
  keep compiling; `bool ok = port->send(x);` does not, which is deliberate -
  those call sites are the ones that should look at the reason.
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

1. One test per rejection reason, on at least one target each, asserting the
   reason rather than only the refusal.
2. Every target keeps its existing accept/reject tests, now reading
   `accepted()`.
3. `scripts/check_docs_compile.sh` passes, which is what proves the documented
   `if (send(...))` form still compiles.
4. The installed-consumer smoke builds against the new headers.
5. The Python binding's own tests, once its mapping is decided.

## Dependencies of the remaining section 9.1 rows

| Row | Depends on | Note |
| --- | --- | --- |
| C-3.1-1b validation before waiting | D-3 | The fix is to validate before the wait; D-3 is what makes the resulting rejection distinguishable from a queue refusal |
| C-3.2-3 keep-latest on the blocking path | Contract open item (keep-latest policy), then D-3 | Not decidable until keep-latest is either adopted as an opt-in policy or dropped from v0.10 |
| C-3.6-3, C-3.6-4 fanout result | D-3 | Needs the aggregate type defined alongside `SendResult`; zero targets must stay distinguishable |
| C-6.1-1 queued data across a link loss | Event model (audit 9.6), then C-3.8-1 | Whether the queue is discarded is part of the event model; the discard has to be observable, which is the statistics decision |
| C-6.1-2 reason a blocked sender was released | D-1 and D-3 | D-1 says who releases the waiter, D-3 carries the reason out |

## Suggested build order

1. D-1, per target, reusing the shape proven on serial.
2. D-2, which is one guard moved into the shared dispatch plus tests per
   callback kind.
3. D-3, then the fanout aggregate (C-3.6-3/4) immediately after, since callers
   should meet both in the same release.

Each lands as its own change with its own tests; none of them waits on the
still-open C-5.4-3, C-1-1 or event-model decisions.

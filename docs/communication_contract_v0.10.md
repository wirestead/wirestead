# v0.10 Communication API Contract (Design Draft)

**Status: Draft.** This is a design proposal for the v0.10.0 communication
contract, not a description of what any release does today. Current guarantees
remain those in [api_stability.md](api_stability.md),
[callbacks.md](callbacks.md), [error_model.md](error_model.md) and the header
comments.

The proposal has **not** been compared against the implementation. That
comparison comes next, across every transport and server, and it records
differences only: neither the contract nor the code is changed to match the
other until each difference has been decided.

Each rule carries a status:

| Status | Meaning |
| --- | --- |
| **Decided** | Agreed in review; changes need a new decision |
| **Proposed** | Recommended direction, pending confirmation |
| **Open** | Not decided; listed in [Open items](#9-open-items) |

A rule without its own marker takes the default status of its section:

| Section | Default status |
| --- | --- |
| 1 Terms, 2 Ready to send | Proposed |
| 3 Transmission | Proposed |
| 4 Receiving | Proposed |
| 5 Execution | Proposed |
| 6 Lifecycle | Proposed |
| 7 Error delivery | Proposed |
| 8 Next step, 9 Open items | Not rules |

The final contract states every rule as a guarantee, an explicit
non-guarantee, or a caller precondition. Words such as "recommended" or "aim
to" belong to this draft only.

## 1. Terms

| Term | Definition |
| --- | --- |
| **Ready to send** | The transport-specific state in which a send can be accepted. See [section 2](#2-ready-to-send-per-transport). |
| **Accepted** | The library took the request for asynchronous transmission. It does not mean the local write finished, nor that the peer received or processed anything. |
| **Discarded before write** | An accepted request removed before any of it was handed to the local write. |
| **Aborted during write** | An accepted request whose local write had started when it was stopped; some of it may already have been written, and whether the peer received any of it is unknown. |
| **Serial scope** | The unit within which callbacks never run concurrently. See [section 5.1](#51-serial-scopes). |
| **Happens-before** | An ordering guarantee between events, separate from serial execution: two callbacks can be serialized without either being guaranteed to come first. |
| **Reentrancy** | A user callback running synchronously on the caller's stack, inside an API call. Distinct from concurrency: a callback on another thread is not reentrancy. |
| **Connection instance** | One established connection, from the moment it becomes ready to send until it is lost or stopped. A reconnect creates a new instance; so does a restart. |
| **Shutdown complete** | For an object or scope, all of: (1) no user callback of it is running; (2) no callback from the run being stopped will start; (3) any internal work still outstanding either does not touch the object's state or holds its own lifetime independently of the object, so destroying the object is safe; (4) after a restart, nothing left over from the previous run affects the new one. It does not require every internal handler to have run. |

**Decided:** the acceptance result is separate from anything that happens
after acceptance. A return value reports acceptance only. Cancellation,
connection loss or discard that happens later cannot be reported through it.

## 2. Ready to send, per transport

The shared transmission rules below say "ready to send". What that means
differs by transport and must not be read as "the peer is reachable".

| Transport | Ready to send means | It does **not** mean |
| --- | --- | --- |
| TCP / UDS client | Connection established; with TLS, the handshake has completed | – |
| TCP / UDS server session | The session was accepted and has not ended | – |
| UDP | The socket is open and bound (**Open:** and whether a destination is required) | That any peer exists or is listening |
| UDP server virtual session | The virtual session exists | That the remote end is still there; a virtual session ends by the library's own rule, not by a remote disconnect |
| Serial | The port is open | That the attached device is powered or responding |

**Proposed.** The UDP rows stay open until the UDP and UDP server transports
have been examined.

## 3. Transmission

### 3.1 Decision procedure

The outcome of a send is decided in three stages, in this order, rather than
by a table read top to bottom.

1. **Validation.** Argument (null shared buffer, empty payload), size (above
   the per-message maximum, or larger than the whole queue limit), and state
   (not started, stopping, not ready to send). Any failure rejects the request
   immediately; nothing waits.
2. **Call site.** If the request would have to wait and waiting is not
   permitted from where it was called (see [section 5.4](#54-call-site-rules)),
   it is rejected with `WouldBlock`.
3. **Strategy.** The table in [3.2](#32-strategy-decision).

**Proposed.** A wait belongs to the connection instance that was current when
it started. After the wait ends, stages 1 and 3 run again, and acceptance also
requires that the connection instance is **still the same one**: if it ended
while the sender waited, the request is rejected even when a new connection is
already ready to send. Checking the state alone is not enough, because the
state can read "ready" again for a different connection.

Reserving capacity, confirming the connection instance and accepting the
request are one synchronized decision, so that several senders woken together
cannot collectively exceed the limit, and none can land on a connection it did
not wait on.

### 3.2 Strategy decision

| API family | Capacity available | Queue pressure |
| --- | --- | --- |
| `try_send*()` | Accept | Reject `WouldBlock` |
| `send*()`, BestEffort | Accept | Reject `QueueFull` (**Decided**: the default rejects the new request) |
| `send*()`, Reliable | Accept | Wait |
| `send_blocking*()` | Accept | Wait, **regardless of strategy** (**Proposed**) |

A wait ends in one of three ways:

| Event while waiting | Result |
| --- | --- |
| Capacity becomes available | Re-run stages 1 and 3 |
| The connection instance it waited on ends, whether or not a new one is already ready | Reject `NotConnected` or the transport's equivalent |
| `stop()` | Reject `CancelledWhileWaiting` |

- **Decided:** a wait may be unbounded while the channel stays ready and the
  pressure does not clear. The contract says so. A timeout API would be a
  separate, additive function and is not required for v0.10.
- **Decided:** Reliable never removes an accepted request because of queue
  pressure while the channel is ready to send. Connection loss and shutdown are
  governed by [section 6](#6-lifecycle).
- **Proposed:** `send_blocking()` in the `Connecting` state rejects immediately
  with `NotConnected` at stage 1. It does not wait for the connection.
- **Proposed:** keep-latest (accepting a new request by removing older accepted
  ones) is out of the v0.10 required scope. If it is added, it is an explicit
  opt-in policy with its own table of which APIs it applies to. With this
  table, `send_blocking()` never removes older requests on a BestEffort
  channel.

### 3.3 Arguments

- **Proposed:** an empty raw payload is rejected with `InvalidArgument`.
  `send_line("")` is judged after the delimiter is appended, so it sends the
  delimiter alone.
- The per-message maximum and the behavior above it are part of the contract;
  the value itself is a documented constant.

### 3.4 Buffer ownership

| API | On acceptance | On rejection |
| --- | --- | --- |
| `send(view)` family | The caller may modify or free the source once the call returns | Source untouched |
| `*_move()` | Consumed | **Proposed:** not consumed, contents unchanged, for **every** move API |
| `*_shared()` | The library keeps a reference while it needs one and releases it once the request is written or discarded | No reference kept |
| `*_shared()` caller precondition | The caller does not modify the buffer's contents through any other reference | |
| Receive `ctx.data()` | Valid only for the duration of the callback | |

How many copies a send makes is a performance property, not part of this
contract. It belongs in [tuning.md](tuning.md).

### 3.5 Order

- Requests accepted on one connection are written in acceptance order,
  **across all send API families**: mixing `send`, `try_send`, `*_move` and
  `*_shared` does not create separate orders.
- Concurrent senders on different threads are ordered by the library's
  internal acceptance order. A caller that needs its own command order
  serializes its sends.
- The order is of local writes only. It says nothing about UDP delivery order,
  and one TCP `send()` does not correspond to one receive callback on the peer.
- Order is preserved among the requests that are written; requests discarded
  or aborted under [section 6](#6-lifecycle) leave gaps.

### 3.6 Servers and sessions

- A send addressed to one session follows [3.1](#31-decision-procedure) to
  [3.5](#35-order) for that session.
- **Implemented:** a send to several sessions never waits. Each target session
  is decided as `try_send*()` would decide it, so one slow session cannot hold
  the call or delay acceptance by the others.
- **Implemented:** the target set is fixed once, when the call selects its
  sessions. A session that connects after that point is not a target. A target
  that ends **between selection and its own acceptance decision** is rejected
  with `NotReady`. A target that ends **after** it accepted keeps that
  acceptance in the result; what happens to the request then follows
  [6.1](#61-events), as a discard before write or an abort during write. A
  result is never revised after the fact.
- **Implemented:** the result reports, over that fixed target set, the number of
  sessions that accepted, the number that rejected, and a count per rejection
  reason. It does not report a single "representative" reason.
- **Implemented:** a send that addresses zero sessions returns a result
  distinguishable from both acceptance and rejection.
- The C++ API and zero-target rules are documented in
  [server_fanout_results.md](server_fanout_results.md).
- Order is per session. Nothing orders one session's writes against another's.
- Statistics are kept per session, with server totals kept separately.

### 3.7 Result type

- **Decided:** sends return a structured acceptance result. Its name must not
  read as write completion; `Sent` and `Delivered` are ruled out, and
  candidates are `SendAdmission` and `EnqueueResult`.
- **Open:** whether the existing `bool` API is replaced outright or kept
  alongside, decided after checking the effect on real consumers.
- Candidate rejection reasons: `NotStarted`, `Stopping`, `NotConnected`,
  `WouldBlock`, `QueueFull`, `TooLarge`, `InvalidArgument`,
  `CancelledWhileWaiting`.

### 3.8 What happens after acceptance

**Decided:** without per-request completion tracking, what happens after
acceptance is observable through statistics only, and the contract states that
limit. Statistics distinguish *discarded before write* from *aborted during
write*.

**Open:** whether those counts can be exact per request depends on how writes
are batched. The contract does not promise exact counters until that has been
checked.

## 4. Receiving

| Item | Contract |
| --- | --- |
| Receive data lifetime | `ctx.data()` is callback-scoped; a copied context owns its data |
| Framer maximum | A configured value per framer |
| Framer over-limit | **Proposed:** defined by each framer. Resynchronization is not guaranteed in general; a length-prefix framer has no sync word to recover with |
| Batch delivery | A batch is scheduled for delivery at the configured count or latency, whichever comes first |
| Batch latency | **Proposed:** the latency is when delivery is **scheduled**, not when the callback runs. The callback runs when the executor makes progress, so a blocked executor delays it without bound |
| Accumulated receive memory | **Open:** whether reading pauses while callbacks are blocked, leaving flow control to the transport |
| Unbounded items | Any buffer without an upper bound is listed as a limitation rather than left implicit |

## 5. Execution

### 5.1 Serial scopes

| Scope | Callbacks in it |
| --- | --- |
| Client channel | Every callback of that channel |
| Server session | That session's receive, connection and backpressure callbacks |
| Server itself | Listening state, server-level errors, and anything not tied to one session |

- Callbacks within one scope never run concurrently.
- **Open:** whether callbacks in different scopes may run concurrently: session
  against session, and session against the server's own scope. The proposal is
  that they may, with no ordering between scopes.

### 5.2 Happens-before

**Proposed**, within one scope:

1. `on_connect` for a connection happens before any receive callback for it.
2. Receive callbacks for one connection run in the order the data arrived.
3. The last receive callback for a lost connection happens before its
   `on_disconnect`, when one is delivered (see [6.2](#62-on_disconnect)).
4. Nothing of the scope runs after its shutdown is complete.

### 5.3 Reentrancy and concurrency

- **Proposed:** no API runs a user callback synchronously on the caller's
  stack.
- That does **not** mean a callback waits for the call that caused it. A
  callback runs on an executor thread and may start before the API call that
  triggered it has returned, `start()` and `on_connect` included.

### 5.4 Call-site rules

| API | External thread | Inside a callback, same scope | Inside a callback, other scope | Executor thread, outside any Wirestead callback |
| --- | --- | --- | --- | --- |
| `try_send*()` | Allowed | Allowed | Allowed | Allowed |
| Blocking send | Allowed; may wait | Reject `WouldBlock` if it would wait | Same | **Proposed:** reject `WouldBlock` if it would wait on an executor it is running on |
| `stats()` | Allowed | Allowed | Allowed | Allowed |
| `stop()` | Waits for shutdown complete; **every** concurrent caller does (**Decided**) | Requests shutdown and returns without waiting (**Decided**) | **Proposed:** request only, if the target's shutdown needs the executor this thread is running; otherwise as an external thread | Same as the previous column |
| `start()` | Allowed; the caller serializes it against `stop()` | Precondition: not called | Precondition: not called | Precondition: not called |
| Handler registration | **Proposed:** only while stopped | Precondition: not called | Precondition: not called | Precondition: not called |
| Configuration change while running | **Proposed:** only items on an explicit, verified list (**Open:** the list) | Same | Same | Same |
| Destruction | See the precondition below | Precondition: not called | See the precondition below | See the precondition below |
| From an OS signal handler | Not guaranteed (**Decided**) | – | – | – |

- **Decided:** repeated `stop()` and concurrent `stop()` are stated separately.
  Repeated calls are no-ops. Concurrent external calls all return only after
  shutdown is complete. Whether that is cheap to implement is not assumed: it
  interacts with reentrancy, external executors and shutdown waiting.
- **Proposed, destruction precondition:** an object is destroyed only once its
  shutdown is complete and nothing else is accessing it concurrently. A waiting
  shutdown is never performed on an executor thread that the shutdown needs in
  order to make progress.
- **Proposed, `stop()` inside a callback:** once the current callback returns,
  no new callback starts **in the same scope**. Callbacks already running in
  other scopes may continue. Completion for the whole object is observable only
  through an external `stop()` (**Open:** or another completion signal).

### 5.5 Concurrent calls on one object

[5.4](#54-call-site-rules) says where a call may be made from; this table says
which calls may overlap on the **same** object, from different threads.

| Combination | Contract |
| --- | --- |
| `send*()` / `send*()` | Allowed. The library protects its queues and state; order follows [3.5](#35-order) |
| `send*()` / `stop()` | Allowed, with four separate cases: (1) a send whose acceptance decision completes before shutdown begins keeps its acceptance, and only what is still unfinished when shutdown reaches it is discarded or aborted under [6.1](#61-events) - a request already written stays written; (2) a call already waiting for capacity is woken and returns `CancelledWhileWaiting`; (3) a call that observes the shutdown is rejected `Stopping` or `NotStarted`; (4) no request is accepted again until an explicit restart |
| `send*()` / `stats()` | Allowed. The snapshot is observational; its fields are not mutually consistent |
| `stop()` / `stop()` | Allowed; every caller returns after shutdown is complete (**Decided**) |
| `start()` / `stop()` | Precondition: the caller serializes them |
| `start()` / `start()` | Precondition: the caller serializes them |
| Handler registration / `start()` | Precondition: the caller serializes them. Allowing registration only while stopped does not make it safe to register concurrently with a `start()` that is making the object run |
| Handler registration / handler registration | Precondition: the caller serializes them |
| Configuration change / `send*()` | Only for items on the explicit list; each listed item states how it is synchronized and from which point it takes effect (**Open:** the list) |
| Destruction / anything | Precondition: not concurrent, see [5.4](#54-call-site-rules) |

### 5.6 Executors

| Executor | External `stop()` waits for | Preconditions |
| --- | --- | --- |
| Owned by the library | Shutdown complete | Running user callbacks return |
| External, run by the library | Shutdown complete | Running user callbacks return |
| External, run by the caller | **Proposed:** shutdown complete, through the library's own tracking of its outstanding work | Running user callbacks return; the executor keeps running; the caller does not wait for shutdown on a thread of that executor |

User handlers that are not Wirestead callbacks still share the executor with
Wirestead's shutdown work. That is why the blocking restriction in
[5.4](#54-call-site-rules) covers executor threads, not only Wirestead
callbacks.

### 5.7 A callback that does not return

- Its own scope stops making progress.
- Other scopes depend on the executor's thread count and whether they share a
  strand. On a single executor thread, everything on that executor can stop.
- `stop()` is not guaranteed to finish in bounded time while a callback it has
  to wait for has not returned.

### 5.8 Exceptions from callbacks

**Open.** The proposal is that an exception thrown by a user callback does not
propagate out of the library, is logged, and does not close the connection.
Whether it is also reported through `on_error` is open; if it is, an exception
thrown by `on_error` itself is only logged.

## 6. Lifecycle

### 6.1 Events

| Event | Not yet written | Being written | Blocked senders | Callback | Statistics |
| --- | --- | --- | --- | --- | --- |
| Connection established | – | – | – | `on_connect` | Connections |
| Connection lost (remote close, error, idle timeout) | Discarded before write (**Decided**) | Aborted during write; delivery unknown (**Decided**) | Woken; `NotConnected` | `on_disconnect(reason)`, see [6.2](#62-on_disconnect) | Both categories, see [3.8](#38-what-happens-after-acceptance) |
| Reconnecting | – | – | – | **Open:** whether an event exists; the proposal is none, with state observable by query | Retry attempts |
| Reconnected | Nothing from the previous connection | – | – | `on_connect` | – |
| Retries exhausted | – | – | – | `on_error` (terminal) | – |
| `stop()` | Discarded before write | Aborted during write | Woken; `CancelledWhileWaiting` | None, see [6.2](#62-on_disconnect) | Counted apart from connection loss |
| Restart (`stop()` then `start()`) | – | – | – | – | Handlers and configuration kept, statistics reset (the existing #444 contract) |
| Server session ended | Discarded before write | Aborted during write | That session's senders woken | `on_disconnect(session, reason)` | Session statistics closed, server totals updated |
| UDP server virtual session expired | Discarded before write | Aborted during write | That session's senders woken | **Open:** distinct from a remote disconnect, which UDP cannot observe | – |

- **Decided:** data from a previous connection is never sent on a new one.
  Offline buffering, keeping unsent data across a reconnect, is allowed only as
  an explicitly supported opt-in feature.
- **Proposed:** after retries are exhausted, the only way back is `stop()`
  followed by `start()`.

### 6.2 `on_disconnect`

**Proposed**, as its own design choice:

> `on_disconnect` fires when a connection that was established during
> operation is lost. It does not fire when the connection ends because of an
> explicit `stop()`. `on_connect` and `on_disconnect` are therefore not always
> paired.

The rule that nothing runs after shutdown is complete does not by itself
exclude an `on_disconnect` delivered **during** shutdown. Omitting it is a
separate decision, recorded here as such.

### 6.3 `on_error`

**Proposed:** `on_error` fires for terminal failures (retries exhausted, start
failure) and, if [5.8](#58-exceptions-from-callbacks) decides so, for callback
exceptions. Connection loss is reported through `on_disconnect` and not also
through `on_error`. Post-acceptance discards are reported through statistics
only. `on_error` is not raised once per failed send.

## 7. Error delivery

Each kind has exactly one path:

| Kind | Delivered through |
| --- | --- |
| Send argument or size error | The acceptance result's rejection reason ([3.1](#31-decision-procedure) stage 1) |
| Any other rejection of a send | The acceptance result's rejection reason |
| Configuration error | A separate configuration-validation policy (**Open:** exception or result, and at `build()` or `start()`) |
| Discard or abort of one accepted request | Statistics only. No per-request notification ([3.8](#38-what-happens-after-acceptance)) |
| Channel or session connection loss | `on_disconnect(reason)` |
| Terminal failure of the channel | `on_error` |
| Shutdown by `stop()` | No callback; blocked senders get `CancelledWhileWaiting` |

"Statistics only" covers individual requests. It does not contradict the
connection-loss and terminal-failure rows, which report the state of the
channel or session, not the fate of a request.

## 8. Next step: implementation comparison

Every transport and server is compared against this draft: TCP, UDS, UDP and
serial clients, and the TCP, UDS and UDP servers. Each rule gets one of:
**matches**, **documentation change**, **implementation change**, **open**.
Differences are recorded, not fixed as they are found. Observations made
earlier on the TCP client were an investigation of one transport and are not
assumed to hold for the others.

## 9. Open items

1. The existing `bool` send API: replace outright or keep alongside the
   structured result.
2. Result type name.
3. UDP ready-to-send definition, including whether a destination is required.
4. Whether callbacks of different scopes may run concurrently.
5. Completion signal for the whole object after `stop()` inside a callback.
6. List of configuration items that may change while running.
7. Callback exceptions and whether `on_error` reports them.
8. Reconnecting event.
9. UDP server virtual-session expiry event.
10. Configuration validation policy: exception or result, and at `build()` or
    `start()`.
11. Accumulated receive memory while callbacks are blocked.
12. Whether post-acceptance statistics can be exact per request.

# v0.10 Contract Audit: TCP Client and TCP Server

Comparison of the Draft contract in
[communication_contract_v0.10.md](communication_contract_v0.10.md) against the
implementation. **This records differences; it changes neither the contract nor
the code.** A difference from a `Proposed` rule is not a bug by itself: the
contract states an intended direction that has not been agreed as behavior yet.

| | |
| --- | --- |
| Baseline | `d914b8d7c` - both the contract and the implementation are read at that commit |
| Targets in this round | TCP client (channel), TCP server (server scope and sessions) |
| Order followed | concurrency and callbacks and ownership → acceptance, order, queue → shutdown, reconnect, events |
| Remaining | UDS client, UDS server, UDP, UDP server, serial |

## Verification status of the baseline

The contract document merged in #646 **was not exercised by CI**: the only
check on that pull request was `dependabot`, and it was skipped, because a
docs-only change matches no workflow's path filter. The document has been
reviewed, not validated.

Evidence in this audit carries one of three markers, and an unrun check is
never recorded as a pass:

| Marker | Meaning |
| --- | --- |
| `[code]` | Read in the source at the baseline commit |
| `[test-exists]` | A test covering it exists; not necessarily run here |
| `[test-run]` | Run locally for this audit, with the result stated |

Tests run for this audit, on Linux/WSL2, Release, at the baseline commit:
`ctest -R "Contract|StopContract"` - **53 tests, all passed**. That selection
includes `ContractComplianceTest.*` and `StopContractTest.*` plus everything
else carrying a contract label. No other suite was run in this round; the rest
of the repository's tests are neither claimed to pass nor to fail here.

## Verdicts

| Verdict | Meaning |
| --- | --- |
| **Match** | The implementation behaves as the rule says |
| **Differs** | The implementation behaves differently; no judgement yet about which side should change |
| **Insufficient evidence** | Reading the code did not settle it; needs a test or a deeper read |
| **Not applicable** | The rule does not apply to this target |

Rule IDs are `C-<section>-<n>`, numbered within the contract section they come
from. One ID appears on more than one row when the same rule is judged
separately for the client and for the server; two different rules never share
an ID.

Counting the rows of the three tables below:

| Verdict | Rows |
| --- | --- |
| Match | 25 |
| Differs | 19 |
| Insufficient evidence | 6 |
| **Total** | **50** |

## 1. Concurrency, callbacks, ownership

| Rule ID | Contract status | Target | Observed implementation | Evidence | Verdict | User impact |
| --- | --- | --- | --- | --- | --- | --- |
| C-5.5-1 `send`/`send` concurrent on one object | Proposed | Client | Wrapper takes a shared lock; the transport serializes on its strand and reserves capacity atomically | `wrapper/tcp_client/tcp_client.cc`, `transport/base/bp_utils.hpp` `[code]` | Match | – |
| C-5.5-1 | Proposed | Server | `broadcast()`/`send_to_client()` hold `sessions_mutex_`; each session writes on its own strand | `transport/tcp_server/tcp_server.cc:886-950`, `tcp_server_session.hpp:102` `[code]` | Match | – |
| C-5.5-2 `send`/`stop`: an accepted request keeps its acceptance | Proposed | Client | A request accepted on the caller thread and then reaching a stopped channel on the strand is counted as a **failed send**, not as a discard of an accepted request | `transport/tcp_client/tcp_client.cc:1219-1232` `[code]` | Differs | The same request is reported as both accepted and failed; a caller reconciling counters sees them disagree |
| C-5.5-3 A waiting caller is woken with `CancelledWhileWaiting` | Proposed | Client | The waiter is woken (`started_ = false`, `bp_cv_.notify_all()`) and the call returns `false`; no reason is carried | `wrapper/tcp_client/tcp_client.cc:244-250, 334-344` `[code]` | Differs | Callers cannot tell shutdown from queue failure; follows from the `bool` return (C-3.7-1) |
| C-5.4-1 External `stop()` waits for shutdown complete | Proposed | Client | The calling thread joins the owned io thread; with an external io_context it does **not** wait for handlers, and relies on flags plus `alive_marker_` to suppress later callbacks | `transport/tcp_client/tcp_client.cc:339-363, 1378-1397` `[code]`; `ContractComplianceTest.TcpClient_StopSemantics` `[test-run, passed]` | Insufficient evidence | The test covers "no callback after stop" for one caller on an external io_context; it does not cover outstanding internal work, which is what the contract's definition adds |
| C-5.4-2 **Every** concurrent `stop()` caller returns after shutdown is complete | Decided | Client | The first caller performs the shutdown; a concurrent second caller returns immediately from the `stop_requested_` / `started_` exchange, before the first has finished | `transport/tcp_client/tcp_client.cc:339-341`; `wrapper/tcp_client/tcp_client.cc:239-250` `[code]` | Differs | A second caller cannot treat the return as shutdown complete; it returns while the first caller is still tearing the object down |
| C-5.4-2 | Decided | Server | Same shape: `stopping_.exchange(true)` returns early for the second caller | `transport/tcp_server/tcp_server.cc:505-508`; `wrapper/tcp_server/tcp_server.cc:285-291` `[code]` | Differs | As above |
| C-5.4-3 `stop()` inside a callback requests shutdown without waiting | Decided | Client, Server | Both skip the join when called on their own io thread; the server additionally runs cleanup directly when already on the executor | `transport/tcp_client/tcp_client.cc:1378-1385`; `transport/tcp_server/tcp_server.cc:522-525` `[code]` | Match | – |
| C-5.4-4 A blocking send inside **any** callback returns `WouldBlock` rather than waiting | Proposed | Client | The guard that makes a blocking send fail fast is set only around data and message dispatch, not around `on_connect`, `on_disconnect`, `on_error` or `on_backpressure` | `wrapper/callback_guard.hpp:31-50`; `wrapper/tcp_client/tcp_client.cc:454` `[code]` | Differs | A blocking send from `on_connect` or `on_error` can wait on the thread that would have to clear the backpressure |
| C-5.4-4 | Proposed | Server | Same: the guard is set in `on_multi_data` only | `wrapper/tcp_server/tcp_server.cc:479` `[code]` | Differs | As above |
| C-5.4-5 A blocking send from an executor thread outside any callback is rejected | Proposed | Both | The check is a thread-local callback-depth counter, so a user handler on the same executor that is not a Wirestead callback is not detected | `wrapper/callback_guard.hpp:31-50` `[code]` | Differs | Contract rule is stricter than the mechanism that exists |
| C-5.3-1 No user callback runs synchronously on the caller's stack | Proposed | Client | Sends `dispatch()` onto the strand, which runs inline when the caller is already on it, and that path can reach `report_backpressure()` and the user's `on_backpressure` | `transport/tcp_client/tcp_client.cc:418-424, 1256-1262, 1270-1296` `[code]` | Insufficient evidence | Reachability was read, not observed; a targeted test would settle it |
| C-5.1-1 Callbacks of one scope never run concurrently | Proposed | Client | One strand per channel | `transport/tcp_client/tcp_client.cc` `[code]` | Match | – |
| C-5.1-2a A session's receive, backpressure and close callbacks run on the session strand | Proposed | Server | `on_bytes`, the backpressure notification and `on_close` are all invoked from session code bound to that session's strand | `tcp_server_session.hpp:102`, `tcp_server_session.cc:33, 378, 440-485` `[code]` | Match | – |
| C-5.1-2b **All** user callbacks of one session are serialized against each other | Proposed | Server | The session owns a strand, but the connect callback runs on the accept path (C-5.2-1) and batched delivery runs on a server-wide path (C-5.1-3), so a strand's existence does not by itself serialize every callback attributed to the session. On the default single io thread nothing can overlap, which is a property of the thread count, not of the scope | `transport/tcp_server/tcp_server.cc:452-470`; `wrapper/tcp_server/tcp_server.cc:469-520` `[code]` | Insufficient evidence | Needs a multi-threaded-executor test before the scope model can be called implemented |
| C-5.1-3 Session callbacks belong to the session scope | Proposed | Server | With batching enabled, contexts from **all** sessions accumulate in one server-wide queue and are delivered together by whichever session's strand fills the batch, or by the batch timer on the server executor | `wrapper/tcp_server/tcp_server.cc:469-520` `[code]` | Differs | Batched delivery is a server-level event carrying several sessions' data; the contract's scope model has no place for it yet |
| C-5.2-1 `on_connect` precedes that connection's receive callbacks | Proposed | Server | The session starts reading before `on_multi_connect` is invoked, and the connect callback runs on the accept path rather than on the session strand | `transport/tcp_server/tcp_server.cc:452-470` `[code]` | Insufficient evidence | Ordering holds on a single io thread by construction; with a multi-threaded executor it was not established |
| C-3.4-1 `send(view)` may release the source once the call returns | Proposed | Client, Server | The payload is copied into a pooled buffer or a vector before the call returns | `transport/tcp_client/tcp_client.cc:379-441`; `tcp_server_session.cc:110-150` `[code]` | Match | – |
| C-3.4-2 A rejected `*_move()` leaves the source unchanged | Proposed | Client, Server | The vector is moved into the strand lambda only after every rejection path has returned | `transport/tcp_client/tcp_client.cc:443-476, 523-568`; `tcp_server_session.cc:159-268` `[code]` | Match | The header comment on `ChannelInterface::send_move()`/`try_send_move()` says the opposite ("consumed regardless of the return value"), so the documentation, not the code, is what differs |
| C-3.4-3 `*_shared()` holds a reference only while it needs one | Proposed | Client | The buffer is released when the write batch is cleared after completion, or when queues are cleared | `transport/tcp_client/tcp_client.cc:1030-1100` `[code]` | Match | – |
| C-5.5-4 `stats()` is observational, fields not mutually consistent | Proposed | Client | A snapshot of independent atomics | `transport/tcp_client/tcp_client.cc:367-371` `[code]` | Match | – |
| C-5.5-4 | Proposed | Server | Server totals plus a per-session sum taken under `sessions_mutex_`, with peak queue depth taken as a max rather than a sum | `transport/tcp_server/tcp_server.cc:705-729` `[code]` | Match | – |

## 2. Acceptance, order, queue

| Rule ID | Contract status | Target | Observed implementation | Evidence | Verdict | User impact |
| --- | --- | --- | --- | --- | --- | --- |
| C-3.1-1a Stage 1 rejects empty and oversized payloads without waiting - `try_send*()` and BestEffort `send*()` | Proposed | Client | The call reaches the transport directly, which rejects zero-length and above-maximum before any queue work | `wrapper/tcp_client/tcp_client.cc:290-319`; `transport/tcp_client/tcp_client.cc:386-399, 523-540` `[code]` | Match | – |
| C-3.1-1b Same rule - Reliable `send*()` and `send_blocking*()` | Proposed | Client | The wrapper waits for backpressure to clear **before** calling the transport, and only the transport validates the payload. While pressure is active, an empty or oversized payload waits first and is rejected afterwards | `wrapper/tcp_client/tcp_client.cc:334-344, 359-376, 400-411`; `transport/tcp_client/tcp_client.cc:386-399` `[code]` | Differs | An invalid request can block the calling thread for as long as the queue stays full, instead of failing at once |
| C-3.1-2 A wait belongs to the connection instance it started on | Proposed | Client | The wait predicate reads `started_`, the channel pointer and `is_backpressure_active()`. Nothing identifies the connection, so a sender that waited through a disconnect and reconnect can be accepted onto the new connection | `wrapper/tcp_client/tcp_client.cc:334-344, 359-376` `[code]` | Differs | This is the case raised in review: a request meant for one connection can land on the next one |
| C-3.2-1 `try_send*()` rejects under queue pressure | Proposed | Client, Server | Rejects on `backpressure_active`, over high-water, or over the hard limit, with an atomic reservation | `transport/tcp_client/tcp_client.cc:523-568`; `bp_utils.hpp:26-40` `[code]`; `ContractComplianceTest.TcpClient_Backpressure_Contract` `[test-run, passed]` | Match | – |
| C-3.2-2 BestEffort `send*()` rejects the new request | Proposed | Client | The wrapper routes BestEffort `send()` to the try path, which rejects | `wrapper/tcp_client/tcp_client.cc:316-319` `[code]` | Match | – |
| C-3.2-3 `send_blocking()` never removes older accepted requests | Proposed | Client | On a BestEffort channel `send_blocking()` uses the plain transport path, which trims the oldest queued buffers to make room (keep-latest) | `transport/tcp_client/tcp_client.cc:404-441`; `bp_utils.hpp:243-289` `[code]` | Differs | On a BestEffort channel, a blocking send silently drops earlier accepted messages |
| C-3.2-4 Reliable `send*()` waits until space, loss or stop | Proposed | Client | Waits, but only for a bounded number of attempts (5); after that it returns false on a healthy channel | `wrapper/tcp_client/tcp_client.cc:357-376, 400-411` `[code]` | Differs | A Reliable send can fail without the channel being stopped or disconnected |
| C-3.2-5 `send_blocking()` in `Connecting` rejects immediately | Proposed | Client | `send_blocking()` checks `started_` and the channel pointer but not readiness, and the transport accepts while connecting, so the request is queued for a connection that does not exist yet. `try_send*()` and `send_move()` do check | `wrapper/tcp_client/tcp_client.cc:400-411, 334-344` `[code]` | Differs | Behavior differs between API families in the same state |
| C-3.5-1 One acceptance order across all send families | Proposed | Client | One `tx_` deque, with a `pending_` deque used only while backpressure is active; the try path is rejected in exactly that state, so it cannot overtake | `transport/tcp_client/tcp_client.cc:1219-1264` `[code]` | Match | – |
| C-3.6-1 Fanout never waits | Proposed | Server | `broadcast()` uses the per-session try path for every session | `transport/tcp_server/tcp_server.cc:886-918` `[code]` | Match | – |
| C-3.6-2 The target set is fixed at selection | Proposed | Server | The loop runs while holding `sessions_mutex_`, so sessions accepted during the call are not targets | `transport/tcp_server/tcp_server.cc:889-898` `[code]` | Match | – |
| C-3.6-3 The result reports accepted and rejected counts per reason | Proposed | Server | Returns a single `bool`: true when at least one session accepted | `transport/tcp_server/tcp_server.cc:890-900` `[code]` | Differs | A caller cannot tell "every session accepted" from "one of fifty accepted" |
| C-3.6-4 A call with zero targets is distinguishable | Proposed | Server | Returns `false`, the same value as "every session rejected", and records one failed send | `transport/tcp_server/tcp_server.cc:899-900` `[code]` | Differs | "No clients connected" reads as a send failure |
| C-3.6-5 Order and statistics are per session | Proposed | Server | Each session owns its queue, strand and counters; the server aggregates | `tcp_server_session.cc`; `transport/tcp_server/tcp_server.cc:705-729` `[code]` | Match | – |
| C-3.7-1 Sends return a structured acceptance result | Decided | Client, Server | Every send API returns `bool` | `wrapper/ichannel.hpp` `[code]` | Differs | Expected: the contract asks for a type that does not exist yet. Rejection reasons are currently unavailable to callers |
| C-3.8-1 Statistics distinguish discarded-before-write from aborted-during-write | Proposed | Client | Counters are `dropped_messages`/`dropped_bytes` and `failed_sends`, which do not encode when the request was lost. `perform_stop_cleanup()` clears both queues without counting what it dropped | `transport/tcp_client/tcp_client.cc:1322-1341`, `diagnostics/runtime_stats_counter.hpp` `[code]` | Differs | Data discarded by `stop()` is invisible in statistics, so the one observation channel the contract allows for post-acceptance loss does not cover the shutdown case |

## 3. Shutdown, reconnect, events

| Rule ID | Contract status | Target | Observed implementation | Evidence | Verdict | User impact |
| --- | --- | --- | --- | --- | --- | --- |
| C-6.1-1 On connection loss, not-yet-written requests are discarded | Decided | Client | The queue survives the loss. A write that failed mid-batch is returned to the front of the queue, and the reconnect path does not clear it, so queued data is written on the next connection | `transport/tcp_client/tcp_client.cc:1061-1075, 1102-1131, 715-816` `[code]` | Differs | The contract's "data from a previous connection is never sent on a new one" does not hold here; a partially written message can also be resent in full, which the peer may see as a duplicate |
| C-6.1-2 Blocked senders are woken with `NotConnected` | Proposed | Client | Connection loss does not clear `backpressure_active_`, and the wait predicate does not test readiness, so a blocked sender is not necessarily woken by the loss itself | `transport/tcp_client/tcp_client.cc:1102-1131`; `wrapper/tcp_client/tcp_client.cc:334-344` `[code]` | Insufficient evidence | Whether the waiter stays blocked depends on timing the code alone did not settle; needs a test |
| C-6.1-3 Connection loss during operation fires `on_disconnect` | Proposed | Client | The wrapper maps `Closed` to `on_disconnect` and `Error` to `on_error`; a loss that leads to a retry transitions to `Connecting`, which the wrapper ignores | `wrapper/tcp_client/tcp_client.cc:505-545`; `transport/tcp_client/tcp_client.cc:1102-1131` `[code]` | Differs | A client that reconnects can deliver `on_connect` twice with no `on_disconnect` in between |
| C-6.1-2 | Proposed | Server | `do_close()` drains the session's queues and clears backpressure before clearing callbacks, which wakes a caller blocked in `send_to_blocking()` for that client | `tcp_server_session.cc:455-470` `[code]` | Match | – |
| C-6.1-3 | Proposed | Server | A session's `on_close` fires the multi-client disconnect handler | `transport/tcp_server/tcp_server.cc:414-421` `[code]` | Match | – |
| C-6.2-1 `stop()` does not fire `on_disconnect` | Proposed | Client | State notifications are suppressed once stopping | `transport/tcp_client/tcp_client.cc:1399-1400` `[code]`; `StopContractTest.*`, `ContractComplianceTest.TcpClient_StopSemantics` `[test-run, passed]` | Match | – |
| C-6.2-1 | Proposed | Server | The session close handler returns early when the server is stopping | `transport/tcp_server/tcp_server.cc:417-418` `[code]`; `ContractComplianceTest.TcpServer_StopSemantics` `[test-run, passed]` | Match | – |
| C-6.1-4 Retries exhausted fires `on_error` | Proposed | Client | The retry decision moves the link to `Error`, which the wrapper maps to `on_error` | `transport/tcp_client/tcp_client.cc:943-950`; `wrapper/tcp_client/tcp_client.cc:505-545` `[code]` | Match | – |
| C-6.3-1 Connection loss is not also reported through `on_error` | Proposed | Client | A failed write records error info for `last_error` but raises no `on_error` | `transport/tcp_client/tcp_client.cc:1061-1075` `[code]` | Match | Combined with C-6.1-3, a loss that is retried produces no callback at all |
| C-6.1-5 Restart keeps handlers and configuration and resets statistics | Proposed | Client | The wrapper keeps handlers and config; the transport resets its state and statistics on start | `transport/tcp_client/tcp_client.cc:1362-1376` `[code]`; wrapper lifecycle tests `[test-exists]` | Match | Already the documented #444 contract |
| C-6.1-6 Session end: statistics closed and folded into server totals | Proposed | Server | `stats_.absorb()` runs under the same lock as the erase, so it happens exactly once | `transport/tcp_server/tcp_server.cc:428-438` `[code]` | Match | – |
| C-1-1 Shutdown complete covers outstanding internal work | Proposed | Server | `stop()` dispatches cleanup onto the io_context and waits up to 2 seconds, then runs cleanup directly. The contract's own precondition for a caller-run executor is that the executor keeps running, so the timeout path is outside the case the rule covers. Separately, `cleanup_started_` records only that cleanup **began**, so whether a second path can return while cleanup is still in progress was not settled | `transport/tcp_server/tcp_server.cc:476-478, 544-560` `[code]` | Insufficient evidence | Needs the three parts separated - running callbacks, cleanup itself, and internal work still outstanding - before any verdict |

## Cross-cutting observations

1. **Three separate gaps, not one.** They all read as "the caller is told
   less than the contract assumes", but they need different changes:

   | Gap | What would close it |
   | --- | --- |
   | The reason for an immediate rejection is unavailable (C-5.5-3, C-3.7-1) | The acceptance result type |
   | Partial acceptance of a fanout is unavailable (C-3.6-3, C-3.6-4) | An aggregate result for multi-session sends |
   | Data discarded after acceptance by `stop()` is unavailable (C-3.8-1) | Counting on the shutdown path |

   The third cannot be solved by any return value: the call has already
   returned by then.
2. **Rules that differ per API family rather than per transport.** C-3.2-3,
   C-3.2-5 and C-5.4-4 differ inside the TCP client itself, between
   `send()`, `send_blocking()`, `try_send()` and the move and shared
   variants. Any of these decided as contract will need the families aligned,
   not one transport fixed.
3. **The reconnect model is the largest single difference.** C-6.1-1 and
   C-6.1-3 together describe a client that keeps queued data across a
   reconnect and reports neither the loss nor the resend. The contract
   describes the opposite. This is a design decision, not a defect to patch
   during the audit.
4. **The event model has a gap the contract does not name.** With C-6.1-3 and
   C-6.3-1 both as observed, an application sees no callback at all for a
   connection loss that is retried successfully.

## Next

1. Confirm or revise the rules marked Differs, one decision per rule, before
   any code changes.
2. Close the Insufficient evidence rows with targeted tests: reentrancy
   through `dispatch()` (C-5.3-1), a blocked sender across a disconnect
   (C-6.1-2), callback ordering on a multi-threaded executor (C-5.2-1), and
   shutdown completion with an external io_context (C-5.4-1).
3. Extend the same table to UDS client, UDS server, UDP, UDP server and
   serial.

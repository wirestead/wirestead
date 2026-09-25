# v0.10 Contract Audit

**Historical baseline, not current status.** See the
[current seven-target conformance report](communication_contract_v0.10_status.md)
for resolved rules, gaps and current validation. Counts and observations below
are preserved at the original baseline.

Comparison of the Draft contract in
[communication_contract_v0.10.md](communication_contract_v0.10.md) against the
implementation. **This records differences; it changes neither the contract nor
the code.** A difference from a `Proposed` rule is not a bug by itself: the
contract states an intended direction that has not been agreed as behavior yet.

| | |
| --- | --- |
| Baseline | `d914b8d7c` - both the contract and the implementation are read at that commit |
| Targets | TCP client, TCP server, UDS client, UDS server, UDP, UDP server, serial - all seven |
| Order followed | concurrency and callbacks and ownership → acceptance, order, queue → shutdown, reconnect, events |
| Round 1 | TCP client and TCP server (sections 1-3), merged as #647 |
| Round 2 | UDS, UDP and serial (sections 4-8), with the categories in section 9 |

Round 2 reads each target's own path from the public wrapper down to the
transport. A verdict is never carried over from TCP because the code looks
similar; where a target does behave identically, the row says so with its own
evidence.

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
`ctest -R "Contract|StopContract"` - **53 tests, all passed**. `-R` filters on
test **names**, so the selection is every test whose name contains "Contract"
or "StopContract", which includes `ContractComplianceTest.*` and
`StopContractTest.*`. No other suite was run in this round; the rest of the
repository's tests are neither claimed to pass nor to fail here.

## Verdicts

| Verdict | Meaning |
| --- | --- |
| **Match** | The implementation behaves as the rule says |
| **Differs** | The implementation behaves differently; no judgement yet about which side should change |
| **Insufficient evidence** | Reading the code did not settle the behavior; needs a test or a deeper read |
| **Comparison deferred** | The contract item is still Open, so there is nothing settled to compare against. The observed behavior is recorded; it does not become the decision |
| **Not applicable** | The rule does not apply to this target |

Rule IDs are `C-<section>-<n>`, numbered within the contract section they come
from. One ID appears on more than one row when the same rule is judged
separately for the client and for the server; two different rules never share
an ID.

Row counts are stated at the end of each round's tables and in
[section 9](#9-summary-by-category).

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
| C-6.1-2a A session ending releases callers waiting on it | Proposed | Server | `do_close()` drains the session's queues and clears backpressure before clearing callbacks, which wakes a caller blocked in `send_to_blocking()` for that client | `tcp_server_session.cc:455-470` `[code]` | Match | – |
| C-6.1-2b The released caller is told `NotConnected` | Proposed | Server | `send_to_blocking()` returns `false`; no reason is carried | `wrapper/tcp_server/tcp_server.cc:389-410` `[code]` | Differs | Same root as C-3.7-1: the reason exists inside the library but has no way out |
| C-6.1-3 | Proposed | Server | A session's `on_close` fires the multi-client disconnect handler | `transport/tcp_server/tcp_server.cc:414-421` `[code]` | Match | – |
| C-6.2-1 `stop()` does not fire `on_disconnect` | Proposed | Client | State notifications are suppressed once stopping | `transport/tcp_client/tcp_client.cc:1399-1400` `[code]`; `StopContractTest.*`, `ContractComplianceTest.TcpClient_StopSemantics` `[test-run, passed]` | Match | – |
| C-6.2-1 | Proposed | Server | The session close handler returns early when the server is stopping | `transport/tcp_server/tcp_server.cc:417-418` `[code]`; `ContractComplianceTest.TcpServer_StopSemantics` `[test-run, passed]` | Match | – |
| C-6.1-4 Retries exhausted fires `on_error` | Proposed | Client | The retry decision moves the link to `Error`, which the wrapper maps to `on_error` | `transport/tcp_client/tcp_client.cc:943-950`; `wrapper/tcp_client/tcp_client.cc:505-545` `[code]` | Match | – |
| C-6.3-1 Connection loss is not also reported through `on_error` | Proposed | Client | A failed write records error info for `last_error` but raises no `on_error` | `transport/tcp_client/tcp_client.cc:1061-1075` `[code]` | Match | Combined with C-6.1-3, a loss that is retried produces no callback at all |
| C-6.1-5 Restart keeps handlers and configuration and resets statistics | Proposed | Client | The wrapper keeps handlers and config; the transport resets its state and statistics on start | `transport/tcp_client/tcp_client.cc:1362-1376` `[code]`; wrapper lifecycle tests `[test-exists]` | Match | Already the documented #444 contract |
| C-6.1-6 Session end: statistics closed and folded into server totals | Proposed | Server | `stats_.absorb()` runs under the same lock as the erase, so it happens exactly once | `transport/tcp_server/tcp_server.cc:428-438` `[code]` | Match | – |
| C-1-1 Shutdown complete covers outstanding internal work | Proposed | Server | `stop()` dispatches cleanup onto the io_context and waits up to 2 seconds, then runs cleanup directly. An executor that never runs at all is outside the contract's precondition for a caller-run executor, but the timeout can also be reached **while the executor is running**, when a long handler or a backlog delays the cleanup - and that path's completion guarantee is unverified. Separately, `cleanup_started_` records only that cleanup **began**, so whether a second path can return while cleanup is still in progress was not settled | `transport/tcp_server/tcp_server.cc:476-478, 544-560` `[code]` | Insufficient evidence | Needs the three parts separated - running callbacks, cleanup itself, and internal work still outstanding - before any verdict |

## Round 1 counts (TCP client, TCP server)

| Verdict | Rows |
| --- | --- |
| Match | 25 |
| Differs | 20 |
| Insufficient evidence | 6 |
| **Total** | **51** |

## 4. UDS client

| Rule ID | Contract status | Target | Observed implementation | Evidence | Verdict | User impact |
| --- | --- | --- | --- | --- | --- | --- |
| C-3.1-1a Validation without waiting - `try_send*()`, BestEffort `send*()` | Proposed | UDS client | The wrapper calls the transport directly; the transport rejects an unconnected channel, an empty payload and, on the try path, an oversized one | `wrapper/uds_client/uds_client.cc:367-392`; `transport/uds/uds_client.cc:389-405` `[code]` | Match | – |
| C-3.1-1b Same rule - Reliable `send*()`, `send_blocking*()` | Proposed | UDS client | The wrapper waits for backpressure first and validates afterwards, as on TCP | `wrapper/uds_client/uds_client.cc:296-307, 354-365` `[code]` | Differs | An invalid request can wait before being rejected |
| C-3.1-1c Oversized payload on the plain path | Proposed | UDS client | `async_write_move()`/`async_write_shared()` check only for an empty payload; an oversized one is stopped by the queue-limit reservation rather than by a size rule, so the rejection reason differs from the try path's | `transport/uds/uds_client.cc:350-388` `[code]` | Differs | Two paths reject the same payload for different stated reasons; only observable once reasons are reported (C-3.7-1) |
| C-3.1-2 A wait belongs to its connection instance | Proposed | UDS client | The wait predicate does include `is_connected()`, so a disconnect ends the wait - but nothing identifies *which* connection, so a wait that outlives a reconnect can still be accepted onto the new one | `wrapper/uds_client/uds_client.cc:296-303` `[code]` | Differs | Same hazard as TCP, one step smaller: the disconnect is at least noticed |
| C-3.2-1 `try_send*()` rejects under pressure | Proposed | UDS client | Shared backpressure helpers, as on TCP | `transport/uds/uds_client.cc:398-440`; `transport/base/bp_utils.hpp` `[code]` | Match | – |
| C-3.2-3 `send_blocking()` never removes older accepted requests | Proposed | UDS client | The plain path routes through the shared `decide_enqueue()`, which trims oldest-first for BestEffort | `transport/uds/uds_client.cc:768`; `bp_utils.hpp:243-289` `[code]` | Differs | Same as TCP |
| C-3.2-4 Reliable `send*()` waits until space, loss or stop | Proposed | UDS client | Bounded to five attempts, then returns false | `wrapper/uds_client/uds_client.cc:314-341` `[code]` | Differs | Same as TCP |
| C-3.2-5 `send_blocking()` in a not-ready state rejects immediately | Proposed | UDS client | The wrapper re-checks `is_connected()` after the wait and before writing, and the transport rejects when not connected | `wrapper/uds_client/uds_client.cc:354-365`; `transport/uds/uds_client.cc:351` `[code]` | Match | Differs from TCP, where the same call queues instead |
| C-3.4-2 A rejected `*_move()` leaves the source unchanged | Proposed | UDS client | The vector is moved only after every rejection path | `transport/uds/uds_client.cc:350-370, 398-440` `[code]` | Match | – |
| C-5.4-2 Every concurrent `stop()` caller returns after shutdown | Decided | UDS client | `stopping_.exchange(true)` returns early for the second caller | `transport/uds/uds_client.cc:277-279` `[code]` | Differs | Same as TCP |
| C-5.4-3 `stop()` inside a callback requests shutdown without waiting | Decided | UDS client | The join is skipped when called on the io thread | `transport/uds/uds_client.cc:297-303` `[code]` | Match | – |
| C-5.4-4 A blocking send inside **any** callback returns `WouldBlock` | Proposed | UDS client | The guard is set only around the data and message dispatch | `wrapper/uds_client/uds_client.cc:460` `[code]` | Differs | Same as TCP |
| C-6.1-1 Queued data is discarded on connection loss | Decided | UDS client | The queue survives: `do_write()` clears it only when stopping, and the reconnect path does not clear it. The batch that was being written **is** dropped, not re-queued | `transport/uds/uds_client.cc:651-690` `[code]` | Differs | Queued data crosses connections, as on TCP; unlike TCP, a partly written message is not resent |
| C-6.1-2 Blocked senders are woken with `NotConnected` | Proposed | UDS client | Woken, because the wait predicate tests `is_connected()`; the reason is not carried, since the call returns `bool` | `wrapper/uds_client/uds_client.cc:296-303` `[code]` | Differs | The wake half works; the reason half is the `bool` gap |
| C-6.1-3 Connection loss during operation fires `on_disconnect` | Proposed | UDS client | `schedule_retry()` transitions to `Error` first, which the wrapper maps to **`on_error`**; `on_disconnect` fires only on `Closed`/`Idle`, which is the give-up path | `transport/uds/uds_client.cc:594-615`; `wrapper/uds_client/uds_client.cc:419-448` `[code]` | Differs | A retried loss is reported, but as an error rather than a disconnect - the opposite failure from TCP, which reports nothing |
| C-6.3-1 Connection loss is not also reported through `on_error` | Proposed | UDS client | Every retryable loss raises `on_error` | same as above `[code]` | Differs | `on_error` fires repeatedly during ordinary reconnect cycles |

## 5. UDS server

| Rule ID | Contract status | Target | Observed implementation | Evidence | Verdict | User impact |
| --- | --- | --- | --- | --- | --- | --- |
| C-3.6-1 Fanout never waits | Proposed | UDS server | `broadcast()` funnels into `async_try_write_shared()`, which uses each session's try path | `transport/uds/uds_server.cc:504-520` `[code]` | Match | – |
| C-3.6-2 The target set is fixed at selection | Proposed | UDS server | The loop holds `sessions_mutex_` | `transport/uds/uds_server.cc:509-518` `[code]` | Match | – |
| C-3.6-3 The result reports accepted and rejected counts | Proposed | UDS server | One `bool`, true when at least one session accepted | `transport/uds/uds_server.cc:510-520` `[code]` | Differs | Same as TCP server |
| C-3.6-4 A call with zero targets is distinguishable | Proposed | UDS server | Returns `false` and records a failed send | `transport/uds/uds_server.cc:517-519` `[code]` | Differs | Same as TCP server |
| C-3.4-2 A rejected `*_move()` leaves the source unchanged | Proposed | UDS server | `async_try_write_move()` moves the vector into a `shared_ptr` **before** any check, so the source is consumed even when the call rejects | `transport/uds/uds_server.cc:499-502` `[code]` | Differs | Unique to this target: a caller that retries after a rejection retries with an empty buffer |
| C-5.1-2a Session receive and close callbacks run on the session strand | Proposed | UDS server | Traced per path: the read handler is `bind_executor(strand_, ...)` and invokes `on_bytes_` inside it; `do_close()` - which invokes `on_close_` - is reached only from strand-bound handlers; handler registration is dispatched onto the strand too | `transport/uds/uds_server_session.cc:29, 48, 276-306, 299-340, 442` `[code]` | Match | – |
| C-5.1-2b All of one session's callbacks are serialized | Proposed | UDS server | Connect and batched delivery run outside the session strand, as on TCP | `wrapper/uds_server/uds_server.cc:330-345` `[code]` | Insufficient evidence | Same open question as TCP server |
| C-5.4-2 Every concurrent `stop()` caller waits | Decided | UDS server | `stopping_.exchange(true)` returns early for the second caller | `transport/uds/uds_server.cc:202` `[code]` | Differs | Same as TCP |
| C-5.4-4 Blocking send inside any callback | Proposed | UDS server | Guard set only in the data dispatch | `wrapper/uds_server/uds_server.cc:342` `[code]` | Differs | Same as TCP |
| C-6.1-6 Session end folds statistics into server totals | Proposed | UDS server | Absorbed under the same lock as the erase | `transport/uds/uds_server.cc:672-690` `[code]` | Match | – |
| C-1-1 Shutdown complete covers outstanding internal work | Proposed | UDS server | Same dispatch-then-timeout shape as the TCP server | `transport/uds/uds_server.cc:202-245` `[code]` | Insufficient evidence | Same open question as TCP server |

## 6. UDP

The contract's connection rules do not transfer. UDP has no peer connection,
so "ready to send" is the socket state plus a destination, and nothing
corresponds to a reconnect.

| Rule ID | Contract status | Target | Observed implementation | Evidence | Verdict | User impact |
| --- | --- | --- | --- | --- | --- | --- |
| C-2-1 Ready to send is socket open and bound | Open in the contract | UDP | `is_connected()` reports an internal flag set when the socket is open and bound, or when the first datagram arrives. The APIs that use the **default** destination additionally require `remote_endpoint_` - configured, or learned from a received datagram - while `async_write_to()` / `async_try_write_to()` take a destination as an argument and do not | `transport/udp/udp.cc:280, 325-327, 849, 984-990, 1102-1130` `[code]` | Comparison deferred | Observation for contract open item 3: whether a destination belongs in the readiness definition is a decision the contract has not made |
| C-3.1-1b Validation without waiting - Reliable and blocking paths | Proposed | UDP | Same wrapper shape: wait first, validate in the transport | `wrapper/udp/udp.cc:318-331, 345-353` `[code]` | Differs | Same as TCP |
| C-3.2-1 `try_send*()` rejects under pressure | Proposed | UDP | Shared helpers, with `TxItem` carrying the destination | `transport/udp/udp.cc:984-1029`; `bp_utils.hpp` `[code]` | Match | – |
| C-3.2-3 `send_blocking()` never removes older accepted requests | Proposed | UDP | The plain path routes through `decide_enqueue()` with a projection over `TxItem`, so BestEffort trims oldest-first | `transport/udp/udp.cc:586-592` `[code]` | Differs | Same as TCP |
| C-3.4-2 A rejected `*_move()` leaves the source unchanged | Proposed | UDP | Moved only after the checks | `transport/udp/udp.cc:984-1015` `[code]` | Match | – |
| C-5.4-2 Every concurrent `stop()` caller waits | Decided | UDP | `stop_requested_.exchange(true)` returns early | `transport/udp/udp.cc:791-793` `[code]` | Differs | Same as TCP |
| C-5.4-4 Blocking send inside any callback | Proposed | UDP | Guard set only in the data dispatch | `wrapper/udp/udp.cc:412` `[code]` | Differs | Same as TCP |
| C-6.1-1 Queued data discarded on connection loss | Decided | UDP | No connection exists to lose | – | Not applicable | The corresponding UDP event is a socket error, which moves the channel to `Error`; queue handling there is covered by C-1-1 |
| C-6.1-3 Connection loss fires `on_disconnect` | Proposed | UDP | No connection loss; a socket error transitions to `Error`, which the wrapper maps to `on_error` | `transport/udp/udp.cc:197-234`; `wrapper/udp/udp.cc:474-490` `[code]` | Not applicable | – |
| C-6.1-4 Retries exhausted fires `on_error` | Proposed | UDP | There is no reconnect cycle; a socket error goes straight to `Error` | `transport/udp/udp.cc:197-234` `[code]` | Not applicable | – |
| C-3.1-2 A wait belongs to its connection instance | Proposed | UDP | No connection instance exists. The wait predicate does end on the channel leaving the ready state | `wrapper/udp/udp.cc:345-353` `[code]` | Not applicable | – |

## 7. UDP server

Sessions here are virtual: the wrapper keeps a map of remote endpoints and
expires an entry after a configured silence, which is not a remote disconnect.

| Rule ID | Contract status | Target | Observed implementation | Evidence | Verdict | User impact |
| --- | --- | --- | --- | --- | --- | --- |
| C-2-2 A virtual session is ready to send while it exists | Proposed | UDP server | Sessions are created on the first datagram from an endpoint and refreshed by each further datagram | `wrapper/udp/udp_server.cc:264-322` `[code]` | Match | – |
| C-6.1-7 Virtual session expiry is distinct from a disconnect | Open in the contract | UDP server | A reaper timer removes sessions silent for longer than the configured timeout and fires `on_disconnect` for each | `wrapper/udp/udp_server.cc:186-238` `[code]` | Comparison deferred | Observation for contract open item 9: today expiry reuses `on_disconnect`. What the event should be is undecided, so this is not counted as a difference |
| C-3.6-1 Fanout never waits | Proposed | UDP server | `broadcast()` calls the try path per session endpoint | `wrapper/udp/udp_server.cc:535-546` `[code]` | Match | – |
| C-3.6-2 The target set is fixed at selection | Proposed | UDP server | The loop holds the wrapper's shared lock | `wrapper/udp/udp_server.cc:535-544` `[code]` | Match | – |
| C-3.6-3 The result reports accepted and rejected counts | Proposed | UDP server | A single OR-ed `bool` | `wrapper/udp/udp_server.cc:539-545` `[code]` | Differs | Same as the other servers |
| C-3.6-4 A call with zero targets is distinguishable | Proposed | UDP server | Returns `false`, as when every session rejects | `wrapper/udp/udp_server.cc:539-545` `[code]` | Differs | Same as the other servers |
| C-5.1-2 Session scope | Proposed | UDP server | There is no per-session strand: all sessions are served by the one UDP socket's executor, and session state lives in the wrapper under one mutex. The rule asks that callbacks of **one** session never overlap, which a single shared execution path can satisfy; whether any callback path of one session can overlap another of the same session was not traced | `wrapper/udp/udp_server.cc:88-89, 264-322` `[code]` | Insufficient evidence | A structure without per-session strands is not by itself a difference; it needs the callback paths traced |
| C-5.4-4 Blocking send inside any callback | Proposed | UDP server | Guard set only in the data dispatch | `wrapper/udp/udp_server.cc:258` `[code]` | Differs | Same as TCP |
| C-5.4-2 Every concurrent `stop()` caller waits | Decided | UDP server | The wrapper's `stop()` runs `started.exchange(false)` and then calls the UDP channel's `stop()`, which itself returns early on its own exchange | `wrapper/udp/udp_server.cc:465-490`; `transport/udp/udp.cc:791-793` `[code]` | Differs | Same as the other targets |

## 8. Serial

| Rule ID | Contract status | Target | Observed implementation | Evidence | Verdict | User impact |
| --- | --- | --- | --- | --- | --- | --- |
| C-2-3 Ready to send is the port being open | Proposed | Serial | `opened_` is set after the port opens and is configured; nothing tests the attached device | `transport/serial/serial.cc:354-355` `[code]` | Match | – |
| C-3.1-1b Validation without waiting - Reliable and blocking paths | Proposed | Serial | Wait first, validate in the transport | `wrapper/serial/serial.cc:329-339, 386-398` `[code]` | Differs | Same as TCP |
| C-3.2-3 `send_blocking()` never removes older accepted requests | Proposed | Serial | The plain path routes through `decide_enqueue()` | `transport/serial/serial.cc:175` `[code]` | Differs | Same as TCP |
| C-3.2-5 `send_blocking()` in a not-ready state rejects immediately | Proposed | Serial | The wrapper re-checks `is_connected()` after the wait | `wrapper/serial/serial.cc:386-398` `[code]` | Match | Differs from TCP |
| C-3.4-2 A rejected `*_move()` leaves the source unchanged | Proposed | Serial | Moved only after the checks | `transport/serial/serial.cc:762-784, 818-861` `[code]` | Match | – |
| C-5.4-2 Every concurrent `stop()` caller waits | Decided | Serial | `stopping_.exchange(true)` returns early | `transport/serial/serial.cc:674` `[code]` | Differs | Same as TCP |
| C-5.4-3 `stop()` inside a callback requests shutdown without waiting | Decided | Serial | `stop()` joins the owned io thread **without** checking whether it is the current thread, unlike every other transport. Confirmed at runtime: the join throws `std::system_error` ("Resource deadlock avoided"), and the rest of the shutdown never runs - see [section 10](#10-runtime-confirmation-serial-stop-inside-a-callback) | `transport/serial/serial.cc:683-686` `[code]`; `test/repro/serial_stop_in_callback_repro.cc` `[test-run, see section 10]` | Differs | `stop()` from a serial callback throws instead of requesting shutdown, and a later restart's future never completes |
| C-5.4-4 Blocking send inside any callback | Proposed | Serial | Guard set only in the data dispatch | `wrapper/serial/serial.cc:433` `[code]` | Differs | Same as TCP |
| C-6.1-1 Queued data discarded when the link drops | Decided | Serial | With `reopen_on_error`, the queue survives the reopen; it is cleared only by cleanup or by a queue overflow | `transport/serial/serial.cc:188-194, 455-465, 498-503` `[code]` | Differs | Same as TCP |
| C-6.1-3 Link loss during operation fires `on_disconnect` | Proposed | Serial | With `reopen_on_error` the state goes to `Connecting`, which the wrapper ignores; without it, the state goes to `Error` and `on_error` fires | `transport/serial/serial.cc:491-509`; `wrapper/serial/serial.cc:495-515` `[code]` | Differs | Reopen-on-error behaves like TCP: a recovered drop is reported nowhere |
| C-6.1-2 Blocked senders are woken | Proposed | Serial | The wait predicate tests `is_connected()`, so closing the port releases waiters; the reason is not carried | `wrapper/serial/serial.cc:329-337` `[code]` | Differs | Wake works, reason does not |

## 9. Summary by category

Rows: round 1 (TCP) 51 - 25 match, 20 differs, 6 insufficient evidence.
Round 2 (UDS, UDP, serial) 58 - 17 match, 32 differs, 3 insufficient
evidence, 2 comparison deferred, 4 not applicable. Totals: 109 rows, 42
match, 52 differs, 9 insufficient evidence, 2 deferred, 4 not applicable.

Most round-2 differences are the same few rules repeating across targets,
which is what the categories below separate. The two deferred rows are not
counted as differences: the contract has not decided those items, so there is
nothing to differ from.

### 9.1 Common differences - the same rule differs on every target examined

Each row names the targets whose own rows carry the evidence. Sharing a
helper is a lead, not a verdict: a target is listed only where the public API
path reaching that helper was read for that target.

| Rule | Targets with a row | What differs |
| --- | --- | --- |
| C-5.4-2 concurrent `stop()` | TCP client, TCP server, UDS client, UDS server, UDP, UDP server, serial | The second caller returns from an `exchange` before the first has finished |
| C-5.4-4 blocking send inside a callback | TCP client, TCP server, UDS client, UDS server, UDP, UDP server, serial | The fail-fast guard is set only around the data and message dispatch, not around connect, disconnect, error or backpressure callbacks |
| C-3.1-1b validation before waiting | TCP client, UDS client, UDP, serial (the four client-side targets), and the same shape in the servers' `send_to_blocking()` | The wrapper waits for backpressure and only then lets the transport validate |
| C-3.2-3 keep-latest on the blocking path | TCP client, UDS client, UDP, serial; the TCP and UDS session paths route the same way | The plain path routes through the shared `decide_enqueue()`, which trims oldest-first for BestEffort |
| C-3.7-1 structured acceptance result | every target with a row | Everything returns `bool`; this also causes C-5.5-3, C-6.1-2b, C-3.6-3 and C-3.6-4 |
| C-3.6-3, C-3.6-4 fanout result | TCP server, UDS server, UDP server | One OR-ed `bool`; zero targets is indistinguishable from all-rejected |
| C-6.1-1 queued data across a link loss | TCP client, UDS client, serial | The queue survives the loss and is written on the next connection |
| C-6.1-2 the reason a blocked sender was released | TCP client, UDS client, serial (UDP has no connection loss) | The wake happens, except on the TCP client (see 9.3); the reason never reaches the caller |

The shared layers - the wrapper's blocking-send loop, `bp_utils.hpp`, the
callback guard - are why the same rule repeats, but the rows above are what
supports it.

### 9.2 Differences between API families inside one target

| Family split | Where | What differs |
| --- | --- | --- |
| `try_send*()` vs Reliable `send*()`/`send_blocking*()` | TCP client, UDS client, UDP, serial | Validation before waiting (C-3.1-1a vs C-3.1-1b) |
| `send()` vs `send_blocking()` on a BestEffort channel | TCP client, UDS client, UDP, serial | `send()` rejects the new request; `send_blocking()` takes the plain path and trims older accepted ones |
| Plain path vs try path, payload size | UDS client | The try path enforces the size maximum; the plain path leaves it to the queue-limit reservation |
| `*_move()` rejection | UDS server vs every other target | The UDS server converts the vector into a `shared_ptr` before its checks, so a rejected call still consumes the source |
| `send_blocking()` while not ready | TCP client vs UDS client, UDP, serial | Only the TCP client omits the readiness check after the wait and queues the request instead of rejecting it |
| Default-destination sends vs `*_write_to()` | UDP | The default-destination APIs require `remote_endpoint_`; the explicit-destination ones take it as an argument |

### 9.3 Transport-specific differences

| Target | Difference |
| --- | --- |
| TCP client | The only target that re-queues the batch it was writing when the write failed, so a partly written message is resent on the next connection. Also the only one whose wait predicate does not test readiness, so whether a disconnect alone releases a blocked sender is unconfirmed (C-6.1-2, 9.4) |
| UDS client | A retried loss reports `on_error` because the retry path passes through `Error` - the opposite of TCP, which reports nothing |
| UDS server | `*_move()` consumes the source even when rejected |
| UDP | No connection instance exists, so four connection rules are not applicable. The APIs that use the default destination additionally require one to be set, while `async_write_to()` takes it as an argument; whether a destination belongs in the readiness definition is still open in the contract (comparison deferred) |
| UDP server | Sessions are virtual and share one executor and one mutex, with no per-session strand; whether that still satisfies per-session non-overlap is untraced (9.4). Expiry fires `on_disconnect` today, recorded as an observation against an open contract item |
| Serial | `stop()` joins the owned io thread without checking whether it is the current thread, so `stop()` from a serial callback joins the calling thread with itself. Every other transport checks |

### 9.4 Insufficient evidence and runtime confirmation, with the smallest check for each

Two kinds are listed together: rows whose **behavior** the code did not settle,
and one row judged Differs in code whose **runtime effect** has not been
observed. They are marked in the Kind column.

| Rule | Target | Kind | Claim to settle | Minimal scenario |
| --- | --- | --- | --- | --- |
| C-5.4-1 | TCP client | Insufficient evidence | `stop()` returns only when outstanding internal work can no longer touch the object | External io_context with a slow handler in flight; `stop()` from another thread; assert no handler touches the object afterwards |
| C-5.3-1 | TCP client | Insufficient evidence | A send from inside a callback can invoke `on_backpressure` synchronously | Drive the queue to the threshold, send from within `on_data`, record the call stack depth or a reentrancy flag |
| C-5.1-2b | TCP server, UDS server | Insufficient evidence | All callbacks of one session are serialized | Multi-threaded executor, one session, a slow `on_data`, assert no other callback of that session overlaps |
| C-5.1-2 | UDP server | Insufficient evidence | Callbacks of one virtual session never overlap, despite there being no per-session strand | Two datagrams from one endpoint while `on_data` is slow; assert no overlap for that session |
| C-5.2-1 | TCP server | Insufficient evidence | `on_connect` precedes that connection's receive callbacks | Multi-threaded executor, a client that writes immediately on connect, assert the order per session |
| C-6.1-2 | TCP client | Insufficient evidence | A blocked sender is released by a disconnect | Fill the queue under Reliable, drop the peer, assert the blocked call returns within a bound |
| C-1-1 | TCP server, UDS server | Insufficient evidence | Shutdown completion on the timeout path | Occupy the executor with a long handler so cleanup cannot finish in 2s; assert what `stop()` guarantees on return |
| C-5.4-3 | Serial | ~~Runtime confirmation of a Differs row~~ **done**, see [section 10](#10-runtime-confirmation-serial-stop-inside-a-callback) | `stop()` from a callback joins the current thread | Done: `test/repro/serial_stop_in_callback_repro.cc` |

The serial row stays classified as Differs in its own table: the code path is
clear, and only its runtime effect is unconfirmed. Turning a code-level
difference into insufficient evidence because no test was run would hide it.

### 9.5 Three reporting gaps that need different fixes

| Gap | What would close it |
| --- | --- |
| The reason for an immediate rejection is unavailable (C-5.5-3, C-3.7-1, C-6.1-2b) | The acceptance result type |
| Partial acceptance of a fanout is unavailable (C-3.6-3, C-3.6-4) | An aggregate result for multi-session sends |
| Data discarded after acceptance by `stop()` or by a link loss is unavailable (C-3.8-1) | Counting on the shutdown and loss paths |

The third cannot be solved by any return value: the call has already returned
by then.

### 9.6 The event model is the largest design question

Across targets, a link loss that is recovered is reported three different
ways: nothing at all (TCP client, serial with reopen), `on_error` (UDS
client), or `on_disconnect` for an expiry that is not a disconnect (UDP
server). Whatever the contract decides, this is one decision for all
transports, not a per-transport fix.

## 10. Runtime confirmation: serial `stop()` inside a callback

Reproduction: `test/repro/serial_stop_in_callback_repro.cc`, registered as
`ReproSerialStopInCallback.{stop-nocatch,stop-catch,control,restart-after-callback-stop,restart-control}`.
It reports rather than asserts, so it does not freeze the current behavior into
a test; it fails only if the repro path cannot run or a scenario hangs. Each
scenario runs in a forked child with a parent-side timeout and a ctest
`TIMEOUT`, so a hang would kill the child, not the runner.

Setup: the public `wirestead::serial()` wrapper in its default configuration,
where the transport owns its io_context and io thread; a POSIX
pseudo-terminal supplies the input; `stop()` is called from `on_data`. The
callback signals its own exit from a scope guard, so the flags are read only
after the callback body has been left - by return **or** by exception. That
signal is about the user callback body; it does not say the library finished
its own handling.

Shutdown and restart are separate scenarios on purpose. A restart scenario
that does not get its `start()` back within the bound reports and exits
**without destroying the object**, because destroying it with a call in flight
would make the observation unsafe rather than informative.

Run: Linux/WSL2, Release, at audit baseline `d914b8d7c` plus this
reproduction. `ctest -R ReproSerialStopInCallback` - 5 scenarios, all passed,
meaning each completed as described below.

| Observation point | `stop-nocatch` | `stop-catch` (diagnostic) | `control` |
| --- | --- | --- | --- |
| Callback entry | entered | entered | entered |
| `stop()` call | did not return; threw | threw `std::system_error`: "Resource deadlock avoided" | not called |
| After the callback body was left | the library's own dispatch caught it and logged "Uncaught exception in user callback: Resource deadlock avoided"; `connected()` false | same, minus the log: the diagnostic catch took it first; `connected()` false | `connected()` true |
| Outside `stop()` | returned | returned | returned |
| Destruction | completed, with no call in flight | completed, with no call in flight | completed, with no call in flight |
| Process result | exited 0 | exited 0 | exited 0 |

Restart is observed separately. `start()` itself returns a future, so what is
observed is whether that future completes - keeping "completed with false"
apart from "not ready within the bound":

| Scenario | `stop()` in the callback | Restart after the outside `stop()` | Destruction |
| --- | --- | --- | --- |
| `restart-after-callback-stop` | yes, threw | `start()` returned, but **its future was not ready within 3s** - not a `false` completion, not an exception | not attempted; the child exited deliberately with the asynchronous start still in flight |
| `restart-control` | no | future completed with `true` | completed after a further `stop()` |

The control scenarios are what make the rest meaningful: the same object, the
same pseudo-terminal and the same outside `stop()` restart cleanly when the
callback does not call `stop()`.

### What this settles

- The effect is an exception, not a hang. `std::jthread::join()` on the current
  thread throws `EDEADLK` rather than blocking, so nothing deadlocks.
- Catching the exception is not enough. `Serial::stop()` throws at its join,
  which is before `ioc_.restart()` and before `started_ = false`, and the
  exception then unwinds out of the wrapper's `stop()` before it clears the
  channel's callbacks and releases the channel. The object is left
  half-stopped: a later `stop()` returns, but a restart's future never
  completes.
- What a destruction in that state would do is **not** established here. The
  reproduction deliberately stops rather than destroying an object with a call
  in flight, so only destruction with nothing in flight is observed as
  completing.
- By the criterion agreed for this check - "even if the exception is caught
  inside, an interrupted shutdown is a defect" - this is a defect, not only a
  contract difference.

### Fix

Fixed after this observation, under the narrow contract below; the
reproduction stays in the tree and now records the fixed behavior.

Swapping the join for a detach was **not** the fix: it removes the exception
without saying who waits for the io thread, who keeps the object alive
meanwhile, and when the io_context becomes restartable. The contract
implemented is narrow:

1. `stop()` from inside a callback requests shutdown and returns, without
   joining the thread it is running on.
2. An outside `stop()` afterwards waits for shutdown to complete, even though
   a shutdown was already requested.
3. After that outside `stop()` returns, restart and destruction are allowed.
4. Restarting from inside a callback, before shutdown is complete, is not
   supported.

That is narrower than the still-open C-5.4-3 and C-1-1 decisions, so it did
not wait for them, and it does not pre-empt them: what a caller may do between
a callback-initiated `stop()` and the outside one is still undecided.

`SerialStopInCallbackTest` covers the three points that matter: the
callback's `stop()` does not throw, an outside `stop()` completes, and the
restarted channel receives data again. It fails on the unfixed library, which
was checked by reverting the two source files and re-running it.

With the fix, the reproduction reports `stop-returned-normally` in both
`stop-nocatch` and `stop-catch`, and `future completed with true` in
`restart-after-callback-stop`, matching `restart-control`.

## Next

1. Decide the common differences in [9.1](#91-common-differences---the-same-rule-differs-on-every-target-examined)
   first: they are properties of the shared layers, so one decision each
   settles every target.
2. Decide the event model ([9.6](#96-the-event-model-is-the-largest-design-question)),
   which the per-transport rows cannot settle individually.
3. Write the checks in [9.4](#94-insufficient-evidence-and-runtime-confirmation-with-the-smallest-check-for-each):
   10 target-level entries across 8 rules - 9 insufficient-evidence rows plus
   one runtime confirmation of the serial `stop()` difference. Start with the
   serial one: its risky path is already identified in code, so it does not
   wait on any design decision.
4. Leave the API-family and transport-specific rows
   ([9.2](#92-differences-between-api-families-inside-one-target),
   [9.3](#93-transport-specific-differences)) until the common rules are
   decided; several of them disappear once the shared layers are settled.


## TCP D-1 follow-up in PR #652 (separate from the baseline audit)

The tables above retain their historical observations and verdicts. The TCP
implementation is being updated after reviewed head `757bd87e3`; that commit's
external-executor path did not wait for transport cleanup. Reference ownership
alone did not establish isolation from a later run.

The follow-up waits for actual client/server cleanup, including session cleanup,
keeps the wrapper channel until an outside caller observes completion, and
serializes server accept/retry/cleanup with generation checks for delayed work.
Callback admission and generation switching remain one locked decision.

Current verification results and limitations are recorded in
[the TCP D-1 validation note](tcp_d1_validation.md). Earlier reports of pre-D1
pass/fail counts and 0/400 ms timings are not reused as measurements of this
follow-up.


### UDS D-1 follow-up

The UDS client/server observations above remain the historical audit at its
stated baseline. The follow-up after TCP PR #652 adds callback admission and
generation gates, outside-caller cleanup completion, target-executor
request-only shutdown and session-strand completion. It preserves the socket
path ownership checks. [UDS validation](uds_d1_validation.md) records the new
tests and the failures reproduced against the unchanged pre-fix UDS sources;
the historical rows are not retroactively changed.

### UDP D-1 follow-up

The UDP and UDP-server rows above retain the original audit baseline. The
follow-up after UDS PR #654 gives every outside caller a completion boundary
for transport cleanup, cancelled I/O and admitted wrapper callbacks. It adds
run-generation admission to data, state, backpressure, batching and peer-expiry
paths; target-executor calls remain request-only. The configured/learned peer
distinction and existing datagram filtering are preserved.
[UDP validation](udp_d1_validation.md) records the real-I/O regression tests,
before/after source comparison and local validation. Serial and D-2/D-3 remain
separate work.

### Serial D-1 follow-up

The serial rows above remain observations at the original audit baseline.
The follow-up after UDP PR #655 extends the narrower callback-stop fix from
PR #650 to every outside caller and to external/shared executors. Completion
includes admitted wrapper callbacks, transport cleanup and tracked cancelled
I/O. Retry and receive-idle callbacks are explicitly dispatched on the strand;
gather-write storage stays valid until its operation releases it.
[Serial validation](serial_d1_validation.md) records PTY and injected-port
tests, a source-verified before/after comparison, and local validation.
D-2/D-3 and actual serial-hardware validation remain separate.

## Post-audit update: payload validation before waiting

C-3.1-1b now bypasses capacity waiting for empty raw payloads and payloads above
MAX_BUFFER_SIZE on all seven wrappers. The existing transport path still
performs the rejection and its accounting. An appended newline counts toward
the maximum and an empty line remains valid. The original observations above
describe the audited baseline; this update covers the payload-shape and
per-message-size subset only. Whole-queue-limit validation, state ordering and
D-3 result reasons remain outstanding. See
[validation evidence](send_validation_before_wait.md).

## Post-audit update: readiness before waiting

TCP no longer waits on stale queue pressure when its channel is disconnected,
and UDP server no longer waits on global queue pressure for an unknown client
ID. Regression coverage checks all four client wrappers and all three server
wrappers. This does not establish connection-instance fencing or the full
synchronized acceptance decision. See
[readiness validation](send_readiness_before_wait.md).

## Post-audit update: whole-queue hard limits before waiting

For the built-in transports, the remaining whole-queue hard-limit portion of
C-3.1-1b now bypasses capacity waiting. All seven wrappers use limits reported
by the transport/session rather than reconstructing them from wrapper
configuration. Existing transport rejection and accounting remain in place.
Custom channels without metadata retain the previous behavior. Lower
path-specific thresholds, structured reasons and connection fencing are not
covered by this update. See
[queue-limit validation](send_queue_limit_before_wait.md).

## Post-audit update: UDP server blocking admission

The UDP server path-specific threshold exception recorded above is resolved
for blocking-capable sends. After waiting, they now use ordinary write
admission; try-send and BestEffort sends retain their nonblocking path.
[UDP admission validation](udp_server_reliable_admission.md) includes real
datagram receipt above the pressure watermark. This is not D-3 completion.

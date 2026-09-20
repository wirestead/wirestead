# v0.10 Contract Audit

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
| **Insufficient evidence** | Reading the code did not settle it; needs a test or a deeper read |
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
| C-5.1-2a Session callbacks run on the session strand | Proposed | UDS server | Each session owns `net::make_strand(ioc_)` | `transport/uds/uds_server_session.cc:29, 48` `[code]` | Match | – |
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
| C-2-1 Ready to send is socket open and bound | Proposed (Open in the contract) | UDP | `is_connected()` reports an internal flag set when the socket is open and bound, or when the first datagram arrives; **every send also requires a destination** (`remote_endpoint_`), configured or learned from a received datagram | `transport/udp/udp.cc:280, 325-327, 849, 984-990` `[code]` | Differs | The contract's UDP row names the socket state only; the implementation also requires a destination, which settles contract open item 3 as a question of wording |
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
| C-6.1-7 Virtual session expiry is distinct from a disconnect | Open in the contract | UDP server | A reaper timer removes sessions silent for longer than the configured timeout and fires **`on_disconnect`** for each | `wrapper/udp/udp_server.cc:186-238` `[code]` | Differs | The implementation already answers contract open item 9, but by reusing `on_disconnect`, which the contract distinguishes from a remote disconnect |
| C-3.6-1 Fanout never waits | Proposed | UDP server | `broadcast()` calls the try path per session endpoint | `wrapper/udp/udp_server.cc:535-546` `[code]` | Match | – |
| C-3.6-2 The target set is fixed at selection | Proposed | UDP server | The loop holds the wrapper's shared lock | `wrapper/udp/udp_server.cc:535-544` `[code]` | Match | – |
| C-3.6-3 The result reports accepted and rejected counts | Proposed | UDP server | A single OR-ed `bool` | `wrapper/udp/udp_server.cc:539-545` `[code]` | Differs | Same as the other servers |
| C-3.6-4 A call with zero targets is distinguishable | Proposed | UDP server | Returns `false`, as when every session rejects | `wrapper/udp/udp_server.cc:539-545` `[code]` | Differs | Same as the other servers |
| C-5.1-2 Session scope | Proposed | UDP server | There is no per-session strand: all sessions are served by the one UDP socket's executor, and session state lives in the wrapper under one mutex | `wrapper/udp/udp_server.cc:88-89, 264-322` `[code]` | Differs | The contract's session scope does not describe this target; virtual sessions share one scope |
| C-5.4-4 Blocking send inside any callback | Proposed | UDP server | Guard set only in the data dispatch | `wrapper/udp/udp_server.cc:258` `[code]` | Differs | Same as TCP |

## 8. Serial

| Rule ID | Contract status | Target | Observed implementation | Evidence | Verdict | User impact |
| --- | --- | --- | --- | --- | --- | --- |
| C-2-3 Ready to send is the port being open | Proposed | Serial | `opened_` is set after the port opens and is configured; nothing tests the attached device | `transport/serial/serial.cc:354-355` `[code]` | Match | – |
| C-3.1-1b Validation without waiting - Reliable and blocking paths | Proposed | Serial | Wait first, validate in the transport | `wrapper/serial/serial.cc:329-339, 386-398` `[code]` | Differs | Same as TCP |
| C-3.2-3 `send_blocking()` never removes older accepted requests | Proposed | Serial | The plain path routes through `decide_enqueue()` | `transport/serial/serial.cc:175` `[code]` | Differs | Same as TCP |
| C-3.2-5 `send_blocking()` in a not-ready state rejects immediately | Proposed | Serial | The wrapper re-checks `is_connected()` after the wait | `wrapper/serial/serial.cc:386-398` `[code]` | Match | Differs from TCP |
| C-3.4-2 A rejected `*_move()` leaves the source unchanged | Proposed | Serial | Moved only after the checks | `transport/serial/serial.cc:762-784, 818-861` `[code]` | Match | – |
| C-5.4-2 Every concurrent `stop()` caller waits | Decided | Serial | `stopping_.exchange(true)` returns early | `transport/serial/serial.cc:674` `[code]` | Differs | Same as TCP |
| C-5.4-3 `stop()` inside a callback requests shutdown without waiting | Decided | Serial | `stop()` joins the owned io thread **without** checking whether it is the current thread, unlike every other transport | `transport/serial/serial.cc:683-686` `[code]` | Differs | Calling `stop()` from a serial callback joins the calling thread with itself; the runtime effect was read, not observed, so it needs a test |
| C-5.4-4 Blocking send inside any callback | Proposed | Serial | Guard set only in the data dispatch | `wrapper/serial/serial.cc:433` `[code]` | Differs | Same as TCP |
| C-6.1-1 Queued data discarded when the link drops | Decided | Serial | With `reopen_on_error`, the queue survives the reopen; it is cleared only by cleanup or by a queue overflow | `transport/serial/serial.cc:188-194, 455-465, 498-503` `[code]` | Differs | Same as TCP |
| C-6.1-3 Link loss during operation fires `on_disconnect` | Proposed | Serial | With `reopen_on_error` the state goes to `Connecting`, which the wrapper ignores; without it, the state goes to `Error` and `on_error` fires | `transport/serial/serial.cc:491-509`; `wrapper/serial/serial.cc:495-515` `[code]` | Differs | Reopen-on-error behaves like TCP: a recovered drop is reported nowhere |
| C-6.1-2 Blocked senders are woken | Proposed | Serial | The wait predicate tests `is_connected()`, so closing the port releases waiters; the reason is not carried | `wrapper/serial/serial.cc:329-337` `[code]` | Differs | Wake works, reason does not |

## 9. Summary by category

Rows: round 1 (TCP) 51 - 25 match, 20 differs, 6 insufficient evidence.
Round 2 (UDS, UDP, serial) 57 - 17 match, 34 differs, 2 insufficient
evidence, 4 not applicable. The round-2 differences are mostly the same
handful of rules repeating across targets, which is what the categories below
separate.

### 9.1 Common differences - the same rule differs on every target examined

| Rule | Targets | What differs |
| --- | --- | --- |
| C-5.4-2 concurrent `stop()` | all 7 | The second caller returns from an `exchange` before the first has finished; every transport uses the same shape |
| C-5.4-4 blocking send inside a callback | all 7 | The fail-fast guard is set only around the data and message dispatch, not around connect, disconnect, error or backpressure callbacks |
| C-3.1-1b validation before waiting | all 5 client-side targets | The wrapper waits for backpressure and only then lets the transport validate, so an invalid request can wait first |
| C-3.2-3 keep-latest on the blocking path | all 6 queue-owning targets | The plain path routes through the shared `decide_enqueue()`, which trims oldest-first for BestEffort |
| C-3.7-1 structured acceptance result | all 7 | Everything returns `bool`; this also causes C-5.5-3, C-6.1-2b, C-3.6-3 and C-3.6-4 |
| C-3.6-3, C-3.6-4 fanout result | TCP, UDS, UDP servers | One OR-ed `bool`; zero targets is indistinguishable from all-rejected |
| C-6.1-1 queued data across a link loss | TCP client, UDS client, serial | The queue survives the loss and is written on the next connection |
| C-6.1-2 the reason a blocked sender was released | all 5 client-side targets | The wake happens (except on the TCP client, see 9.3); the reason never reaches the caller |

These are properties of the shared layers - the wrapper's blocking-send loop,
`bp_utils.hpp`, the callback guard - rather than of any one transport.

### 9.2 Differences between API families inside one target

| Family split | Where | What differs |
| --- | --- | --- |
| `try_send*()` vs Reliable `send*()`/`send_blocking*()` | all client-side targets | Validation before waiting (C-3.1-1a vs C-3.1-1b) |
| `send()` vs `send_blocking()` on a BestEffort channel | all queue-owning targets | `send()` rejects the new request; `send_blocking()` takes the plain path and trims older accepted ones |
| Plain path vs try path, payload size | UDS client | The try path enforces the size maximum; the plain path leaves it to the queue-limit reservation |
| `*_move()` rejection | UDS server vs every other target | The UDS server converts the vector into a `shared_ptr` before its checks, so a rejected call still consumes the source |
| `send_blocking()` while not ready | TCP client vs UDS client, UDP, serial | Only the TCP client omits the readiness check after the wait and queues the request instead of rejecting it |

### 9.3 Transport-specific differences

| Target | Difference |
| --- | --- |
| TCP client | The only target that re-queues the batch it was writing when the write failed, so a partly written message is resent on the next connection. Also the only one whose blocked senders are not released by a disconnect, because its wait predicate does not test readiness |
| UDS client | A retried loss reports `on_error` because the retry path passes through `Error` - the opposite of TCP, which reports nothing |
| UDS server | `*_move()` consumes the source even when rejected |
| UDP | No connection instance exists, so four connection rules are not applicable. Readiness additionally requires a destination, which the contract's UDP row does not mention |
| UDP server | Sessions are virtual and share one executor and one mutex; there is no per-session serial scope. Expiry fires `on_disconnect`, which the contract treats as a different event |
| Serial | `stop()` joins the owned io thread without checking whether it is the current thread, so `stop()` from a serial callback joins the calling thread with itself. Every other transport checks |

### 9.4 Insufficient evidence, and the smallest test that would close it

| Rule | Target | Claim to settle | Minimal scenario |
| --- | --- | --- | --- |
| C-5.4-1 | TCP client | `stop()` returns only when outstanding internal work can no longer touch the object | External io_context with a slow handler in flight; `stop()` from another thread; assert no handler touches the object afterwards |
| C-5.3-1 | TCP client | A send from inside a callback can invoke `on_backpressure` synchronously | Drive the queue to the threshold, send from within `on_data`, record the call stack depth or a reentrancy flag |
| C-5.1-2b | TCP server, UDS server | All callbacks of one session are serialized | Multi-threaded executor, one session, a slow `on_data`, assert no other callback of that session overlaps |
| C-5.2-1 | TCP server | `on_connect` precedes that connection's receive callbacks | Multi-threaded executor, a client that writes immediately on connect, assert the order per session |
| C-6.1-2 | TCP client | A blocked sender is released by a disconnect | Fill the queue under Reliable, drop the peer, assert the blocked call returns within a bound |
| C-1-1 | TCP server, UDS server | Shutdown completion on the timeout path | Occupy the executor with a long handler so cleanup cannot finish in 2s; assert what `stop()` guarantees on return |
| C-5.4-3 | Serial | `stop()` from a callback joins the current thread | Call `stop()` from `on_data` on an owned io_context; assert the observed behavior |

The last row is new in round 2: the serial finding was read in code, so the
runtime effect is stated as unverified rather than as a defect.

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

## Next

1. Decide the common differences in [9.1](#91-common-differences---the-same-rule-differs-on-every-target-examined)
   first: they are properties of the shared layers, so one decision each
   settles every target.
2. Decide the event model ([9.6](#96-the-event-model-is-the-largest-design-question)),
   which the per-transport rows cannot settle individually.
3. Write the tests in [9.4](#94-insufficient-evidence-and-the-smallest-test-that-would-close-it),
   eight rows across seven rules.
4. Leave the API-family and transport-specific rows
   ([9.2](#92-differences-between-api-families-inside-one-target),
   [9.3](#93-transport-specific-differences)) until the common rules are
   decided; several of them disappear once the shared layers are settled.

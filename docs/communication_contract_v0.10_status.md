# v0.10 current policy conformance

**Full conformance: not established; known gaps remain.** Passing the current
test suite does not imply that the entire design draft is implemented.

Reviewed on 2026-09-25 against core 638d6a427 (PR #684), Python 948e1c4
(PR #72), and the UDS move-ownership correction accompanying this report.
The [original audit](communication_contract_v0.10_audit.md) is a historical
comparison at d914b8d7c. Its 109 rows and totals are not current bug counts.

This report covers all 51 distinct rule IDs in the original seven-target audit
and the additional draft clauses in the section inventory below. A rule not
proved by a targeted test is not promoted to verified simply because a whole
suite passes. No Proposed/Open rule is silently made Decided by this report.

## Transport and session accounting follow-up

The built-in TCP/UDS clients and Serial now implement optional logical-request accounting:
[API, epoch semantics and evidence](tcp_send_accounting.md). Its stop/loss/
queue-pressure discards and active aborts are separate, with exactly-once
termination and gather-prefix attribution. [UDP socket accounting](udp_send_accounting.md)
now also covers default-peer and explicit-destination datagrams. TCP/UDS server
sessions and aggregation now have the same ledger, exactly-once retirement and
reset handling. UDP virtual sessions now expose peer projections; expiry discards
waiting work and preserves active outcomes. The shared ledger retains expired
contributors, including late completions, without a second aggregation step.
TCP implementation baseline validation: 1,957 full-suite cases discovered (1,945 passed, 12 existing UDP
skips); all 71 new accounting cases passed 100 repeats each and AddressSanitizer
with leak detection. The installed shared-library consumer smoke also passed.
UDS/Serial extension evidence: 224 controlled-interface cases passed with
AddressSanitizer and leak detection, covering all seven input families on both
transports. Full-suite and repeated-run results are recorded with the PR.
UDP extension evidence: 120 cases cover ten default-peer/explicit-destination
input configurations, with 100 successful repeats each. Full-suite and sanitizer
results are recorded with the PR.
UDP virtual-session evidence: 27 integration cases cover try and pooled/fallback
blocking sends, controlled active-completion expiry boundaries, pending storage, reset, stop/restart,
peer isolation and final-admission stop. The full suite discovered 2,571 cases:
2,559 passed and the same 12 UDP diagnostic skips remained. All 15 ledger and
147 UDP cases passed 100 repeats each and AddressSanitizer with leak detection.
Installed shared-library and external session-layout/API smoke checks passed.
The baseline observations below are retained except where explicitly updated.

## Evidence and verdicts

- **Covered**: the named behavior has implementation and regression evidence.
  This is a bounded conclusion, not proof for every interleaving or custom transport.
- **Gap**: current source differs from the planned behavior. A gap against a
  Proposed rule still needs a policy decision; a gap against a Decided rule
  is implementation debt.
- **Open**: design or runtime evidence is insufficient for sign-off.
- **Partial**: some scopes match; the exceptions are explicit.
- **Fixed here**: a reproduced discrepancy corrected with a failing-before,
  passing-after regression.

Paths below are relative to the repository. Evidence groups:

| Group | Implementation inspected | Regression evidence |
| --- | --- | --- |
| Lifecycle | All seven wrapper stop/start paths; native completion/generation fences; [callback gate](../wirestead/wrapper/callback_guard.hpp) | TCP/UDS/UDP wrapper stop-completion tests, serial stop-completion tests, callback-gate and stop-admission tests |
| Admission | All seven wrapper send_state, validation and blocking loops; native submission locks; [ConnectionChannel](../wirestead/interface/connection_channel.hpp) | test_client_send_results.cc, test_send_result.cc, test_connection_channel.cc, test_send_validation_before_wait.cc, native/targeted lifecycle and stop-admission tests |
| Callback guard | [invoke_user_callback](../wirestead/wrapper/callback_guard.hpp), all seven wrapper dispatch paths | test_callback_blocking_send.cc, test_server_callback_blocking_send.cc, callback exception/gate tests |
| Fanout | [TCP](../wirestead/transport/tcp_server/tcp_server.cc), [UDS](../wirestead/transport/uds/uds_server.cc) broadcast_result; [UDP](../wirestead/wrapper/udp/udp_server.cc) try_broadcast | test_server_broadcast_contract.cc, test_server_broadcast_slow_consumer_contract.cc |
| Ownership | Native four-client admission paths, TCP/UDS sessions; [context copies](../wirestead/wrapper/context.hpp); UDS server move adapters | test_send_buffer_apis.cc, test_connection_channel.cc, test_send_result.cc, test_message_context.cc; UdsMoveOwnershipTest added here |
| Queue policy | Six native routes preserve accepted work; shared reservation/transfer lock; seven wrapper retry loops | BestEffort queue-preservation tests, mixed reservation hard limits, more-than-five retries and callback/terminal release cases |
| Statistics | [RuntimeStats](../wirestead/wrapper/runtime_stats.hpp), [counter implementation](../wirestead/diagnostics/runtime_stats_counter.hpp); each native cleanup/write-completion path | Legacy counter tests plus test_send_accounting.cc and test_tcp_send_accounting.cc, transport_stream_send_accounting.cc and test_udp_send_accounting.cc, transport_session_send_accounting.cc and test_server_send_accounting.cc; discard/abort accounting covers TCP/UDS clients and server sessions/aggregates, Serial, UDP sockets and virtual sessions |
| Events | TCP/UDS/serial retry transitions; each wrapper on_state; UDP server run_reaper | Lifecycle/reconnect tests exercise current behavior, not the proposed unified event contract |

Full file names are searchable under test/unit and test/integration; the
implementation symbols above describe the specific decision being assessed.

## Seven-target scope check

| Target | Completed core path | Remaining target-specific limitations |
| --- | --- | --- |
| TCP client | D-1/D-2, typed admission, sticky connection-pinned waits, no reconnect replay, logical-request accounting | Bare-executor waiting, reconnect event mapping |
| UDS client | Same guarantees traced in its own implementation, including logical-request accounting | Same executor gaps; retried loss can report on_error |
| UDP client | D-1/D-2, typed admission; open socket plus default destination; native-run pin and socket-wide logical accounting | Bare-executor waiting remains; batch timers now share the socket strand |
| Serial | D-1/D-2, typed admission; device-instance pin, no reopen replay and logical-request accounting | Same executor gaps; recovered loss notification; physical-device validation remains separate |
| TCP server | D-1/D-2, typed targeted sends and pinned session waits, fixed fanout aggregate | Executor waiting, session connect/batch ordering |
| UDS server | Same public guarantees; native move rejection fixed here | Same server gaps; legacy native bool fanout is distinct from public FanoutResult |
| UDP server | D-1/D-2, endpoint/run-pinned sends, fixed fanout, peer accounting and waiting-work expiry | Shared socket pressure and serialized callbacks; mixed-peer batch scope and distinct expiry event remain open |

UDP client loss means socket failure/run end, not a remotely detected disconnect.
UDP server virtual expiry must not be treated as proof that the remote endpoint
disconnected. Custom client channels must implement ConnectionChannel; the
framework cannot prove an arbitrary injected implementation obeys that protocol.

## Original audit rule coverage

| Rule | Scope and current verdict | Evidence / remaining condition |
| --- | --- | --- |
| C-1-1 | Covered: shutdown lifetime boundary on all seven wrappers/native targets | Lifecycle: retained handlers and generations isolate old work; executor/callback preconditions remain |
| C-2-1 | Covered: UDP readiness definition used by current admission | Native write_state/admission_rejection distinguishes open socket from configured/learned default target; explicit destination needs no default |
| C-2-2 | Covered for admission and selected expiry policy | Retained session token; waiting work expires, active results remain; unrelated peer and endpoint reuse regressions |
| C-2-3 | Covered: Serial readiness | Native opened device state, not device responsiveness |
| C-3.1-1a | Covered: nonblocking payload validation, all seven wrappers | Admission: empty/null/maximum/whole-queue checks before native admission |
| C-3.1-1b | Covered: validation before capacity waiting, all seven wrappers | Admission regression tests include line delimiters and invalid sizes |
| C-3.1-1c | Covered: UDS plain-write maximum | Native UDS write_copy/move/shared now validate size |
| C-3.1-2 | Covered: original connection/run/session pin | Admission: first terminal cause survives later stop/reconnect; replacements cannot receive the waiting request |
| C-3.2-1 | Covered: explicit try rejects pressure | Native try results retain WouldBlock |
| C-3.2-2 | Covered: ordinary wrapper BestEffort rejects the new request | Wrappers route through try and map WouldBlock to QueueFull; not the explicit blocking path |
| C-3.2-3 | Covered: explicit blocking preserves older accepted work under BestEffort | Shared default routing no longer invokes keep-latest; no opt-in disposal policy introduced |
| C-3.2-4 | Covered: blocking capacity races retry without an attempt limit | Retained pin and brief condition-variable backoff; callback callers do not retry, stop/loss ends waiting |
| C-3.2-5 | Covered: no wait for an unready connection | Admission state validation returns NotReady before capacity polling |
| C-3.4-1 | Covered: copied send input can be released after return | Native copy-before-accept paths and ownership tests |
| C-3.4-2 | Fixed here for native UDS server; covered for client/session APIs | UDS bool fanout used to consume on all-rejected. Now restores the original vector when no session accepts; partial acceptance consumes it |
| C-3.4-3 | Covered for admission ownership; terminal lifetime has bounded evidence | Rejected shared input is not retained; accepted buffers live through write/cancellation handlers. D-1 does not promise every retained internal handler has already executed |
| C-3.5-1 | Covered for inspected native admission paths; not a caller-thread ordering guarantee | Submission lock covers acceptance/reservation/post order; shared FIFO/gather tests. Custom implementations own their protocol guarantee |
| C-3.6-1 | Covered: all public server fanout never waits for capacity | Fanout: one try admission per target, no retries; ordinary locks/allocations are not wait-free |
| C-3.6-2 | Covered: all public server target sets fixed once | TCP/UDS retained session identity; UDP wrapper read lock fixes virtual targets |
| C-3.6-3 | Covered: all public server aggregates | FanoutResult counts every target and each rejection reason |
| C-3.6-4 | Covered: zero targets distinct | empty() is true, counts zero; bool alone intentionally cannot distinguish it from all-rejected |
| C-3.6-5 | Covered for built-in server per-session statistics | UDP peer traffic/accounting exposed; pressure state/events are shared socket values, not summable peer counters |
| C-3.7-1 | Covered for C++ wrappers; explicit compatibility boundary elsewhere | SendResult/FanoutResult replace public bool; low-level bool adapters remain. Python PR #72 deliberately retains bool |
| C-3.8-1 | Covered for built-in clients and server sessions/aggregates | Separate stages/causes and reset epochs; UDP expiry affects waiting work, aggregate retention requires no peer-total copying |
| C-5.1-1 | UDP timer/receive gap closed; native TCP/UDS/Serial client strands unchanged | Built-in UDP timers now share the socket strand; controlled two-runner data/message batch regressions fail before the fix |
| C-5.1-2a | Covered for TCP/UDS session receive/backpressure/close paths | Session strand bindings; this does not establish connect/batch serialization |
| C-5.1-2b | Open evidence: TCP/UDS all callbacks attributed to a session | Connect runs on accept path, batches use a server-level path; need multi-thread overlap probes |
| C-5.1-2 | Covered for built-in UDP receive/batch/expiry callback non-overlap | One shared socket strand serializes these paths, including across peers; two-runner expiry regression verifies a held receive excludes the reaper callback |
| C-5.1-3 | Gap against proposed session-only batch scope | TCP/UDS/UDP server batch queues mix session contexts; scope assignment needs a policy |
| C-5.2-1 | Open evidence: TCP/UDS connect-before-receive | Session start precedes connect-handler invocation on accept path; test with immediate peer data and several executor threads |
| C-5.3-1 | Open evidence for the universal no-inline-callback proposal | Inspected native sends post work, but configuration/start/stop, direct transports and custom channels need separate caller-stack probes |
| C-5.4-1 | Covered: external stop completion, all targets | Lifecycle tests, including externally run executors |
| C-5.4-2 | Covered: every concurrent external stop waits | Lifecycle tests distinguish repeated stop from overlapping callers |
| C-5.4-3 | Covered: target executor/callback stop is request-only | D-1 executor dependence, not a global any-callback shortcut; external stop observes completion |
| C-5.4-4 | Covered: blocking send from any wrapper callback refuses waiting | D-2 guards all callback kinds and nested/cross-channel callbacks |
| C-5.4-5 | Gap against proposal: ordinary tasks on the required executor can wait | Blocking-send loops check callback depth, unlike shutdown_needs_this_thread; no general executor admission guard |
| C-5.5-1 | Covered for native concurrent send admission | Wrapper shared locks plus native submission/reservation locks |
| C-5.5-2 | Covered for built-in clients and server sessions | Logical-request ledger includes UDP virtual sessions; legacy failed_sends retains separate semantics |
| C-5.5-3 | Covered: cancellation while waiting | Admission tests check CancelledWhileWaiting and first-cause retention |
| C-5.5-4 | Covered: observational stats | Legacy fields remain independent atomics; the optional logical-request ledger is a separately consistent snapshot, not atomic with legacy fields |
| C-6.1-1 | Covered for TCP/UDS/Serial no replay; partial for wider event table | Queues and active batches fenced by connection generation. UDP expiry discards waiting endpoint datagrams; active writes retain their outcome |
| C-6.1-2 | Covered: first loss reason released to public callers | NotReady is the implemented name; UDP uses run/virtual-session lifetime |
| C-6.1-2a | Covered: session end releases targeted waiter | Retained session wait record; native/targeted lifecycle tests |
| C-6.1-2b | Covered: released targeted caller gets the reason | Public server SendResult and pinned final admission |
| C-6.1-3 | Partial: server session close event; client event policy differs | TCP/Serial retries can skip on_disconnect; UDS retry passes through Error; plain UDP has no peer loss |
| C-6.1-4 | Partial: terminal error paths exist, unified event semantics open | Retry-exhaustion tests cover current transitions, not exactly-once terminal-event contract across all failures |
| C-6.1-5 | Covered for restart state tested under D-1 | Handlers/config retained, new run resets stats; does not establish arbitrary live-setter safety |
| C-6.1-6 | Covered for built-in server accounting | TCP/UDS transfer once; UDP shared ledger retains expired/stopped contributors with reset fencing and peer projections |
| C-6.1-7 | Partial: expiry cleanup implemented; distinct event open | Waiting work expires, active results remain; on_disconnect is unchanged and does not prove remote disconnect |
| C-6.2-1 | Covered for public wrapper stop callback suppression | Callback gate closes before teardown; direct transport callbacks are a separate boundary |
| C-6.3-1 | Gap against proposed unified events | UDS retry can invoke on_error; TCP/Serial retry can be silent. A result type does not repair event semantics |

## Additional draft clauses, including those without original audit IDs

| Contract section | Current assessment |
| --- | --- |
| 1: accepted versus delivered | Implemented result naming and docs; no delivery guarantee. Post-acceptance classification now exists for TCP/UDS clients, Serial, UDP sockets and virtual sessions |
| 3.2: Reliable never pressure-drops accepted data | Both built-in strategies preserve accepted work under fixed limits; mixed plain/try reservations and pending transfers share hard-limit synchronization |
| 3.3: empty lines and size bounds | Existing validation/line tests cover delimiter-only requests and whole-queue bounds; not an allocation-failure guarantee |
| 3.4 / 4: receive lifetime | MessageContext copy constructor clones borrowed data; move remains cheap. Retaining only a data view beyond callback is unsupported |
| 4: framer limit and resynchronization | Framer-specific implementations/tests exist; no universal recovery guarantee for length-prefix framing |
| 4: batch count/latency | Count and timer flush tests pass. Latency schedules work, not a callback deadline; blocked executors can delay it |
| 4: receive memory/unbounded items | Open: no consolidated bound on aggregate batch/session/context memory, especially when user handlers stall |
| 5.1 / 5.2: cross-scope ordering | Open: assigning mixed-session batches and proving timer/connect/receive ordering requires explicit scope design |
| 5.4: destruction, signals and concurrent start | Caller preconditions remain: no concurrent destruction/use, serialize start/start and start/stop, no signal-handler guarantee; tests do not make unsupported calls safe |
| 5.4 / 5.5: registration and live configuration | Open policy: locks on some setters are not a verified allowlist or a thread-safety promise for all setters; test_live_setter_forwarding.cc only covers selected behavior |
| 5.6 / 5.7: executors and nonreturning handlers | D-1 handles owned/external executors with progress preconditions; cannot promise bounded stop when a user callback never returns |
| 5.8: callback exceptions | Wrapper invoke_user_callback catches/logs standard and unknown exceptions; no recursive on_error forwarding. Direct native callbacks have transport-specific policies; no single finalized exception policy |
| 6.1: retry exhaustion/restart | Current lifecycle tests cover restart after stop. Whether any other restart transition is supported must be stated in the final contract |
| 6.1: cause-separated cleanup statistics | TCP/UDS clients and server sessions/aggregates, Serial and UDP sockets distinguish explicit stop, loss and queue pressure. UDP loss means local socket error; UDP virtual-session expiry is a separate waiting-work discard cause |
| 6.2 / 6.3 / 7: event versus error versus send refusal | Wrapper structured send refusals are implemented; global event taxonomy and configuration error policy are still open |
| 7: configuration validation | Different paths throw, clamp or fail start; exception/result and build/start timing need a documented choice |
| Python | Bool compatibility with old/new core is verified by PR #72; rich result exposure is optional new API work, not an unfinished bool adapter |
| Release | Core pin remains v0.9.6 in Python. No v0.10 release readiness claim while the gaps above remain |

## Completion gates and next work

1. **Accounting evidence:** TCP/UDS client/session and Serial/UDP socket
   ledgers now track logical requests, gather prefixes, terminal causes and reset
   epochs. TCP/UDS aggregates retain closed/retiring contributors exactly once.
   UDP peer projections and expiry attribution are implemented; controlled
   controlled completion gates cover posted/pending/active boundaries and unrelated peers.
2. **Queue semantics implemented:** [selected preservation/retry policy](blocking_queue_policy.md).
   No implicit keep-latest; blocking capacity retries have no attempt limit.
   An explicit freshness policy or timeout API remains optional future work.
3. **Execution scopes:** [UDP timer/receive serialization](udp_callback_serialization.md) is implemented.
   Settle batch/session ownership and add deterministic
   multi-thread callback overlap/order tests. Extend or explicitly limit the
   nonwaiting rule for ordinary executor tasks.
4. **Events:** decide recovered loss versus terminal error, and UDP virtual
   expiry notification. Waiting UDP work now expires; active work keeps its outcome.
5. **Finish the public contract:** live-setter allowlist, receive-memory limits,
   exception/configuration policy, migration docs and satellite release pins.

See [the accounting/event implementation proposal](post_acceptance_policy_v0.10.md)
for a concrete design and acceptance scenarios. Built-in client/session accounting
is implemented; remaining execution and event proposals are not silently approved.

## Validation for this review

- Baseline full Debug suite: 1,882 discovered, 1,870 passed, 12 existing UDP
  diagnostic skips. This is regression evidence, not a full-contract certificate.
- New native UDS ownership cases: all four fail before the correction because
  the caller's source becomes empty on rejection.
- Post-correction full Debug suite: 1,886 discovered, 1,874 passed, the same
  12 UDP diagnostic skips; no failures.
- Four new cases repeated 100 times each: 400 passing executions.
- AddressSanitizer with leak detection: all eight native UDS server/ownership
  cases passed.
- All 51 historical rule IDs are present in this report; relative document
  links and git diff --check passed.

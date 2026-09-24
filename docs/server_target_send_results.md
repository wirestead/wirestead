# TCP/UDS server target admission results

TCP and UDS server sessions retain SendResult internally for copy, move and
shared writes, including try forms. Public session, server and wrapper methods
still return bool. Acceptance means local queue admission, not delivery.

## Native sessions and targeted sends

Session admission reports NotReady for a closed or closing session,
InvalidArgument for empty/null payloads, TooLarge for payloads above
MAX_BUFFER_SIZE, and WouldBlock for insufficient capacity. State checks,
reservation and posting share a submission mutex with stop/close. Rejected
move writes keep their input storage. Ordinary writes reserve the hard queue
limit; try forms use the existing high-water and hard-cap checks.

The server retains the exact session selected by the client ID under its
session-map lock through admission. A separate admission mutex orders
targeted sends with server stop. Never-started and fully stopped servers
report NotStarted, incomplete stop reports Stopping, and absent/closed
sessions report NotReady. These are server-level decisions; a session does
not reinterpret its closure as the lifecycle of the whole server.

Public bool adapters observe the typed result after admission locks release.
Existing session/server failure and BestEffort drop accounting remains at its
original rejection stage. Late asynchronous failure cannot change an already
returned admission result.

## Nonblocking wrappers

Explicit try_send_to and try_send_to_line combine payload validation, wrapper
lifecycle and native session admission. Ordinary BestEffort send_to and
send_to_line use the same combiner, mapping native WouldBlock to QueueFull.
Explicit try forms keep WouldBlock under either strategy.

Wrapper validation comes before state or capacity: empty input is invalid,
a line includes its delimiter, and the available session hard limit bounds
payload size. Early wrapper refusals do not increment native counters.
A wrapper must be started; incomplete stop/callback cleanup yields Stopping
and completed stop yields NotStarted. Missing native server capability
or a missing target yields NotReady.

## Compatibility and remaining scope

The exported session classes gain a mutex, changing their object layout.
Rebuild consumers against the matching library. Public signatures stay bool.
Wrapper prevalidation/start requirements also change observable behavior and
native accounting. UDS ordinary session writes now explicitly validate the
maximum message size before copying/reserving.

Reliable/explicit blocking wrapper sends still use the existing bool retry
path. Per-session first-terminal-cause wait records and final admission pins
across wrapper restart are subsequent work; this change does not claim those
guarantees. Public result-returning interfaces, custom Channel contracts and
fanout aggregation remain pending.

## Verification

Tests cover all six session forms with pooling on/off and both strategies,
rejected move storage, empty/null/oversized payloads, and paused admission
racing stop. TCP/UDS integration tests cover wrapper copy/line forms,
native string/span ordinary/try forms, absent/disconnected targets,
validation/accounting, queue capacity and stop lifecycle.

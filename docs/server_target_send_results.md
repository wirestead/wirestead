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

## Reliable and explicit blocking sends

Reliable send_to/send_to_line and explicit send_to_blocking now keep a single
internal result across validation, waiting and final native admission.
Validation precedes lifecycle and capacity, and includes the line delimiter
and target hard queue limit. A callback scope never waits for capacity or
retries after refusal. Only native WouldBlock is retried, at most five times;
a terminal rejection is returned immediately.

A send retains the exact selected session and wrapper generation at entry.
Each session stores its first terminal wait cause under the admission mutex:
server/wrapper stop selects CancelledWhileWaiting; session closure selects
NotReady. A waiter keeps that cause after map removal, later stop or restart.
Server stop cancels the selected sessions before cleanup can remove them.
The wrapper also cancels waits before publishing its stopped state.

A selected capacity-release result permits another lifecycle/admission check;
it does not promise acceptance. Final native admission verifies that the target
map still contains the retained session. The wrapper also checks its generation,
so an old send cannot cross a wrapper restart. A changed target/run is refused
without submitting data to a replacement session.

Lock order is wrapper, native target admission, session map, then session
admission. Session close releases its admission mutex before invoking callbacks
that can acquire the server map. Wait polling needs only the retained session's
admission mutex, so removal from the map cannot lose the terminal cause.

## Compatibility and remaining scope

The exported session classes gain admission/wait state, changing their object
layout. Rebuild consumers against the matching library. Public signatures
stay bool. Wrapper prevalidation/start requirements change observable behavior
and native accounting, including Reliable/explicit blocking sends.
UDS ordinary session writes explicitly validate the maximum message size
before copying/reserving. Waiting sends no longer follow a replacement run
or retry terminal refusals.

Public result-returning interfaces, custom Channel contracts and fanout
aggregation remain pending. Acceptance is still local admission, not delivery.

## Verification

Tests cover all six session forms with pooling on/off and both strategies,
rejected move storage, empty/null/oversized payloads, and paused admission
racing stop. TCP/UDS integration tests cover wrapper copy/line forms,
native string/span ordinary/try forms, absent/disconnected targets,
validation/accounting, queue capacity and stop lifecycle.

Reliable tests pause the executor while publishing queue pressure, then park
the sender at wait entry, selected release or final native admission. They
cover connection loss before stop, native/wrapper stop before restart,
replacement peers, capacity release before stop/restart, callback refusal and
late loss at final admission. Replacement-session accepted counters stay zero.
Separate tests check five-attempt capacity retries, single callback attempts,
and validation before waiting without incrementing native counters.

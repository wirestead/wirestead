# Executor waits and session-specific server batches

Built-in blocking-capable sends do not wait or retry when called from an ordinary
task on the target I/O context. Available capacity still permits admission.
Pressure or a final-admission capacity race returns WouldBlock. Invalid payloads,
lifecycle failures and retained connection/session outcomes keep their existing
precedence. Ordinary BestEffort and explicit try semantics are unchanged.

The call captures its nonwaiting policy while selecting the run and target.
Detection includes raw io_context executors and strands, including type-erased
strands. Testing the inner context also excludes a task on a sibling strand.
Another independent I/O context is an outside caller and may still wait.
The existing rule for every wrapper user callback, including cross-channel
callbacks, remains in force.

All seven built-in wrappers have this protection. Custom channels using these
executor forms participate as well. Other custom executor types still require
an explicit execution-dependency contract; this is not a universal executor
introspection API.

## Server batch scope

TCP, UDS and UDP data/message batches now belong to one session. The configured
count is per session, and one session's traffic cannot complete another's batch.
Each session owns its timer. TCP/UDS timers use the native session strand; UDP
timers retain the shared socket strand. These choices serialize a timer with
the same session's connection, receive, message, pressure and end callbacks.

A pending timer is not postponed when the other queue receives its first item.
Latency schedules delivery and is not a deadline for completion: held callbacks
and blocked executors can delay it. Different TCP/UDS sessions may overlap;
UDP peers still share one strand. Cross-session parallelism is not guaranteed.

Connection loss and UDP expiry flush a session's partial batches before its end
notification. Explicit stop instead suppresses subsequent callbacks and cleanup
discards retained batches. If a batch callback requests stop, the next message
or end callback is refused. Retired session timers cannot deliver into a new run.
UDP expiry still uses the legacy notification until the approved event-policy
follow-up introduces its distinct event.

Native TCP/UDS disconnect callbacks now execute on their session strand.
A closing session stays visible to shutdown until its notification has returned,
so moving the callback does not remove the external-stop completion boundary.
UDS connection counts/lists exclude closed sessions retained for that wait,
matching TCP; statistics can retain the closing contributor until retirement.

## Compatibility and verification

A batch no longer mixes client IDs, and count-based delivery can occur later
for individually quiet sessions. Consumers relying on server-wide batch counts
must adapt. This is a behavioral compatibility change; send admission results
and connection pinning remain intact.

Controlled tests cover same-context and unrelated-context callers, all client
send forms, admission races, per-session counts, two-runner timer exclusion,
stop from a batch, and partial delivery before session end. Loopback tests cover
all three servers. Baseline comparisons and local/platform validation results
are recorded with the pull request.

Receive-memory limits, the universal no-inline-callback proposal, live-setting
validation and unified events are separate remaining implementation work.

Local validation: 2,632 discovered tests (2,620 passed, 12 existing UDP skips),
207 AddressSanitizer cases, 26 boundary cases repeated 50 times (1,300 passes),
five TLS loopback cases, and installed shared-library/session-layout smoke checks.
The baseline comparison fails 15 of 16 probes; the UDP timer probe already passes
because the previous UDP strand fix covered it. Platform CI is recorded on the PR.

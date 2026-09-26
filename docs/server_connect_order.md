# TCP/UDS server connection callback ordering

Built-in TCP and UDS servers queue connection notification on the new session's
strand before publishing it to senders. The session is alive when it becomes
visible, so sends can still be accepted while the connection callback is running.
Queued session work waits for that callback to return.

Previously, the server started the session and then invoked connection callbacks
on the accept path. With multiple io_context runners, immediate peer data or
send backpressure could invoke a session callback before connection notification
finished. Wrapper framer initialization could also lose the first received bytes.
Connection notification now precedes session receive, framed message and
backpressure callbacks without holding a callback mutex across user code.

## Lifecycle and compatibility

Connection callbacks can send and request stop. Stop on the required executor
remains request-only. A stop requested from connection notification prevents
already available peer data from reaching receive callbacks; external stop waits
for the connection callback and session shutdown to finish. Generation checks
reject stale notification and TCP state work after stop/restart.

TCP initiates its handshake before notification, as before. Handshake completion
always queues read startup on the session strand, even when a plain socket
completes inline. A connection callback is still an accepted TCP connection
notification, not a promise that TLS negotiation has completed.

The public session start entry point remains available. No session data members
or public signatures change. Native server state callbacks still use the server
management strand. Connection callbacks for separate sessions can now overlap
when multiple runners are available; applications must synchronize shared state.
This change does not guarantee parallel execution across sessions.

Server batch timers and mixed-session batch ownership remain a separate policy
gap. This change does not claim that every callback attributed to a session is
serialized, or establish a universal no-inline rule or ordinary executor-task
blocking-send rule.

## Verification

Twelve parameterized regressions cover native and wrapper TCP/UDS servers with
two controlled io_context runners: immediate peer input and send pressure during
a held connection callback, stop from connection notification, and external stop
waiting for notification completion. The first eight fail on the previous
implementation and pass after the ordering correction.

Existing fixed-snapshot fanout and pinned-admission tests now finish connection
initialization before parking send calls under the wrapper lock. They retain the
same assertions for late peers, peer loss, fixed membership and first-cause
rejection. Their hook must not prevent the very initialization needed to observe
the later disconnect.

Local validation: 2,603 discovered tests (2,591 passed, 12 existing UDP
large-payload skips), 371 AddressSanitizer cases, and the 12 new ordering/stop
cases repeated 100 times under AddressSanitizer (1,200 passes). Five TLS loopback
cases and installed shared-library consumer/session layout smoke checks pass.
Platform CI results are recorded on the pull request.

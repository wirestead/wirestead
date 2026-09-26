# Connection loss, terminal failure and UDP expiry

The concrete wrappers distinguish three events:

- An established client connection leaving its ready state emits on_disconnect
  once, even when retry later succeeds. A successful retry emits on_connect.
- Failed start or exhausted/disabled retry emits one on_error. An established
  loss followed by terminal failure therefore produces disconnect then error;
  retry attempts do not produce error callbacks.
- UdpServer::on_session_expired reports local idle expiry of a virtual session.
  It carries the session id and endpoint through ConnectionContext and does not
  assert that the remote endpoint disconnected. UDP expiry no longer invokes
  on_disconnect.

For a UDP client, ready/loss events describe the local socket, not remote
reachability. Server session disconnect behavior remains specific to TCP/UDS.
Transient TCP/UDS accept failures retain Listening and their diagnostic record
while accepting is retried; they are not terminal error events.

Pending client receive batches are delivered before the loss notification;
incomplete framing storage is reset at loss. Receive overflow has already
discarded the affected stream's pending receive queues before entering this
path. Server session batches likewise flush before disconnect or expiry.
If a callback requests stop, further event admission is suppressed.

Explicit stop emits neither disconnect nor expiry. Completed external stop
remains the callback-completion boundary; stop inside a callback remains a
request-only operation. A stopped/restarted wrapper begins a fresh notification
epoch. No new callback concurrency or arbitrary custom-executor guarantee is
introduced here.

## Migration

Applications that previously watched UDS on_error for every failed attempt
should query connection state/diagnostics while retry continues, and treat
on_error as terminal failure. Native UDS retry now remains Connecting and ends
in Error only when it will not retry.

Register UDP expiry on the concrete object, for example by building with
auto_start(false), calling on_session_expired, and then starting. The inherited
on_disconnect signature remains available for source compatibility, but idle
expiry does not invoke it. Existing UDP waiting-send expiry accounting and
active-send completion rules are unchanged.

## Verification

Typed client tests cover recovered loss, terminal failure, initial retries,
duplicate state notifications, restart and stop from loss callbacks. Native
TCP/UDS/Serial/UDP paths exercise real state transitions; TCP uses loopback,
UDS/Serial use controlled native endpoints, and UDP tests local socket failure.
Existing UDP expiry, callback serialization, stop completion and session-batch
ordering tests exercise the new expiry callback.

# Configuration and callback exceptions

Invalid configuration is rejected before the affected value is applied. Concrete
wrapper setters and native configuration entry points throw std::invalid_argument;
builders may also use their existing BuilderException for identity validation.
There is no implicit clamping at native construction. Applications that want
clamping can explicitly call the existing validate_and_clamp utility, then
construct the transport. Validation still rejects malformed identities and
unsupported enumeration values.

Actual connection, bind, device-open and TLS-file failures remain execution
outcomes: failed start results and the lifecycle error event. A TCP server port
of zero requests an ephemeral port; a TCP client remote port of zero is invalid.

## Runtime changes

Configuration outside this allowlist requires completed external stop. Attempting
it during a run or while stop is still completing throws std::logic_error.
Calling stop from a callback only requests stop and does not authorize mutation
of stopped-only configuration within that callback.

| Object | Settings allowed during a run |
| --- | --- |
| All concrete wrappers | Callback registration, framer configuration, batch_size, batch_latency |
| TCP client | backpressure_strategy, retry_interval, max_retries, connection_timeout, idle_timeout, idle_timeout_action |
| UDS client | backpressure_strategy, retry_interval |
| Serial client | backpressure_strategy, retry_interval |
| UDP client | backpressure_strategy |
| TCP/UDS servers | max_clients |
| UDP server | max_clients, backpressure_strategy, idle_timeout |

Existing framer semantics remain: replacing a client framer affects subsequent
receive processing; a server framer factory applies to subsequently created
sessions. Changing batch latency does not retroactively re-arm an already
scheduled batch timer. This allowlist does not make concurrent start/start,
start/stop, destruction/use or arbitrary custom-channel mutation supported.

Read-buffer size, socket options, queue thresholds, receive limits, external
context management are stopped-only.
They previously could appear to succeed while updating only cached configuration
without changing the running transport. Configure them before start or after
completed external stop. auto_start is a lifecycle convenience, not a setting
that bypasses these preconditions.

## User callback failures

Wrapper and native notification callbacks contain and log both standard and
unknown exceptions. A throwing data, state, connection or backpressure callback
does not recursively invoke an error callback and does not prevent independent
notifications or transport cleanup. By default, receiving continues.

The existing native Serial and UDP stop_on_callback_exception option remains
an explicit opt-in to quiet stop after a receive callback failure. It does not
synthesize a terminal error notification. Errors from actual transport operations
still follow the lifecycle policy.

These rules do not make a callback that never returns safe, and do not guarantee
that a custom logging sink successfully records a diagnostic.

## Verification

The configuration/callback update passes the full Debug suite: 2,762 discovered,
2,750 passed and 12 existing UDP diagnostic skips. AddressSanitizer with leak
detection passes 49 affected policy, callback and connection-fence cases; the 28
new policy cases also pass 50 repetitions each (1,400 executions).

All five TLS loopback tests pass, including rejection of an incomplete certificate/key
pair while preserving the previous working TLS configuration.

Installed static and TLS shared-package consumers build and run request/reply, fanout,
session layout, all seven concrete receive-limit APIs, the new UDP expiry
registration and an unchanged telemetry custom framer from wirestead-examples.
The TLS consumer explicitly resolves OpenSSL before the wirestead package, matching
the existing dependency-discovery requirement. These checks do not publish a
release or advance satellite release pins.

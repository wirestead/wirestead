# Error Model

`wirestead` reports runtime failures through explicit return values, callbacks,
and exceptions. The expected path depends on when the failure is detected.

## Lifecycle errors

`start()` returns `std::future<bool>` and `start_sync()` returns `bool`.

- `true`: the channel or server reached its connected/listening state.
- `false`: startup failed. A registered `on_error(...)` callback receives the
  specific failure when the implementation can classify it.

`stop()` is idempotent and blocks until pending async operations are cancelled.
After `stop()` returns, no further callbacks should fire.

## Choosing a send method

| Method | Blocks? | Payload ownership | Use when |
| --- | --- | --- | --- |
| `send(data)` / `send_line(line)` | Strategy-dependent (Reliable blocks, BestEffort doesn't) | Borrows (`string_view`) | Default choice; respects the channel's configured `BackpressureStrategy` |
| `send_blocking(data)` / `send_line_blocking(line)` | Always blocks | Borrows | Need guaranteed delivery for this one call, regardless of strategy |
| `try_send(data)` / `try_send_line(line)` | Never blocks | Borrows | Producer loops, or any call that must never stall the caller (never call from inside a callback while backpressured - see [Callback data lifetime](callbacks.md)) |
| `send_move(data)` / `try_send_move(data)` | Strategy-dependent / never blocks | Transfers (`vector<uint8_t>&&`) | Avoid a copy when you already own the buffer and don't need it back |
| `send_shared(data)` / `try_send_shared(data)` | Strategy-dependent / never blocks | Shared (`shared_ptr<const vector<uint8_t>>`) | Same payload fanned out to multiple channels without copying |

Rule of thumb: start with `send()`. Switch to a `try_*` variant for producer
loops or callback-driven sends where blocking is unacceptable. Switch to a
`_blocking` variant only when a specific call must guarantee delivery
regardless of the configured strategy.

## Send errors

`send(...)` follows the configured `BackpressureStrategy`.

- `Reliable`: waits for queue pressure to clear or uses the transport's
  Reliable pending queue.
- `BestEffort`: may drop when the queue is full.

`try_send(...)`, `try_send_move(...)`, and `try_send_shared(...)` are always
non-blocking. They return `false` when the channel is stopped, disconnected,
backpressured, or full. Reliable `try_send*` calls do not enqueue into pending
queues. A `true` return means the payload was accepted or reserved for send
queue insertion according to the pressure state observed by the transport. It
must not later be rejected only because queue pressure changed before the
transport strand processed the enqueue. A concurrent `stop()` or close can still
cancel already accepted asynchronous work.

`try_send*` rejects payloads that would exceed the current non-blocking queue
threshold. A payload may be rejected even if it is below `MAX_BUFFER_SIZE` when
it is larger than the current backpressure high-water budget. Use `send()` or
`send_blocking()` when Reliable enqueue semantics are required for large
payloads.

`send_blocking(...)` waits until queue pressure is relieved, then enqueues using
the normal strategy-aware write path. It returns `false` if the channel stops
while waiting or cannot accept the write. It can block indefinitely while the
channel remains active and pressure does not clear. Use `try_send(...)` for
producer loops, or call `stop()` from another thread to unblock a blocking
sender.

Use `RuntimeStats` to inspect accepted bytes, sent bytes, failed sends, drops,
queued bytes, pending bytes, and backpressure state.

## Liveness and idle timeout

`idle_timeout(...)` is an application-level stale-session policy, not a
heartbeat protocol.

- `0ms` disables idle timeout.
- TCP client idle timeout closes the current socket. By default it uses
  `IdleTimeoutAction::Reconnect`, so the client follows the existing retry
  interval, max retries, and reconnect policy. `IdleTimeoutAction::Close`
  closes without reconnecting.
- TCP server idle timeout closes only the idle client session. The server keeps
  listening, and a peer that connects again is accepted as a new session.
- UDP server idle timeout removes the virtual session and invokes
  `on_disconnect(...)`. If the endpoint sends another datagram later, it is
  discovered as a new virtual session.

`keep_alive(true)` enables the operating system's TCP keepalive option. Its
detection interval and failure behavior are OS-dependent and should not be used
as the only mechanism for predictable stale-session cleanup.

## RuntimeStats send accounting

Send counters distinguish rejected Reliable sends from intentional BestEffort
drops.

- `send*` or `try_send*` rejected because the channel is stopped, disconnected,
  closed, in error, or the payload is invalid: `failed_sends` increases.
- Reliable `try_send*` rejected because the queue is backpressured or full:
  `failed_sends` increases and `pending_bytes` must not increase.
- BestEffort `try_send*` rejected because the queue is backpressured or full:
  `dropped_messages` and `dropped_bytes` increase and `pending_bytes` must not
  increase.
- Reliable `send*` accepted while backpressure is active may enter the Reliable
  pending queue. `pending_bytes` reflects those bytes until pressure clears and
  the transport flushes them into the active send queue.
- `messages_accepted` and `bytes_accepted` increase when a payload is accepted
  or reserved for asynchronous queue insertion. A later lifecycle cancellation
  after acceptance is reported as a failed send when the transport can observe
  it.

`broadcast(...)` and `try_broadcast(...)` are non-blocking fanout operations.
They return success when at least one recipient accepts the payload. Recipients
that are stopped, disconnected, full, or backpressured are reflected in the
corresponding per-transport `RuntimeStats` counters: Reliable fanout rejects
increase `failed_sends`, while BestEffort fanout drops increase
`dropped_messages` and `dropped_bytes`.

## Async runtime errors

Use `on_error(...)` for production workflows. `ErrorContext` contains:

- `code()`: structured `ErrorCode`
- `message()`: diagnostic message
- `client_id()`: optional server-side client identifier

Transport callbacks may also log internal diagnostic details.

## Validation and configuration errors

Builder and config validation errors may throw `diagnostics::ValidationException`
or related `diagnostics::WiresteadException` types when invalid input is detected
synchronously.

Configuration APIs can also return validation results or boolean status where
the API is designed as a query/update operation.

## Exceptions

Public-facing exceptions should prefer the `diagnostics::WiresteadException`
hierarchy. Some lower-level utility APIs intentionally preserve standard C++
exception behavior, such as `std::out_of_range` for bounds-checked access or
`std::invalid_argument` for invalid memory helper arguments.

Known standard-exception APIs:

- `memory::SafeDataBuffer::operator[]` and `at(...)` throw
  `std::out_of_range` for invalid indexes.
- `memory::SafeDataBuffer` raw-pointer constructors throw
  `std::invalid_argument` for null data with non-zero size or oversize input.
- `memory::SafeDataBuffer::validate()` throws `std::runtime_error` when the
  buffer exceeds the configured safety limit.
- `config::ConfigManager::get(...)` and `get_type(...)` throw
  `std::runtime_error` when a required key is missing. Use `has(...)`,
  `get(key, default_value)`, or `validate(key)` when absence is expected.

## Callback exceptions

Callbacks should not throw. When a callback throws, transports catch the
exception, log it, and may transition to an error state depending on the
transport configuration, such as `stop_on_callback_exception`.

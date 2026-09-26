# Public client send results (v0.10)

ChannelInterface and TcpClient, UdsClient, UdpClient and Serial now return
SendResult from all ten send methods: send, try_send, send_line, try_send_line,
send_blocking, send_line_blocking, send_move, try_send_move, send_shared and
try_send_shared. Acceptance means local queue admission, not delivery.

## Reading the result

Contextual bool checks continue to work. Assignments to bool, bool-returning
adapters, atomic<bool>, promise<bool> and similar consumers must explicitly use
accepted(). Code that needs the rejection should retain SendResult instead.
Call reason() only on rejection.

```cpp
#include <wirestead/wirestead.hpp>

void submit(wirestead::wrapper::ChannelInterface& client) {
  const auto result = client.try_send("hello");
  if (!result) {
    const auto reason = result.reason();
    if (reason == wirestead::SendRejection::WouldBlock) {
      // Schedule a later attempt if this message is still useful.
    }
  }
  const bool accepted = client.send_line("status").accepted();
  (void)accepted;
}
```

The library and all consumers must be rebuilt together: this changes public
virtual return types and ABI even where a caller uses only a bool condition.
Custom ChannelInterface subclasses must update all ten overrides and return
truthful decisions. There is no parallel *_ex wrapper API.

## Admission ordering and ownership

Payload validation runs before lifecycle/readiness and capacity decisions.
Empty borrowed/moved payloads and null/empty shared payloads are InvalidArgument.
A line includes its appended newline; an empty line is therefore a valid
one-byte payload. The maximum message size and the available native/custom
hard queue limit produce TooLarge.

A wrapper must be started, including when an injected channel is already
connected. Never-started and fully stopped wrappers return NotStarted;
incomplete stop/callback cleanup returns Stopping. Transport readiness and
final queue reservation are decided at admission. Early wrapper refusals
do not increment native admission counters.

Explicit try methods never wait and preserve WouldBlock. BestEffort send
methods map capacity WouldBlock to QueueFull. Reliable and explicit blocking
sends retain the original connection and wrapper generation. The first
terminal event during a capacity wait selects NotReady for loss or
CancelledWhileWaiting for stop, and later changes cannot overwrite that
selected rejection. Capacity release only permits another admission attempt.

Only WouldBlock is retried, without an attempt limit on the same pin. A callback
scope cannot wait and attempts final admission at most once. Rejection does
not consume a moved vector or retain shared storage. Late I/O errors cannot
change an already returned result.

## Injected Channel compatibility

The shared_ptr<interface::Channel> constructors require either the corresponding
built-in native transport (TCP client, UDS client, UDP channel or serial) or
interface::ConnectionChannel. Null, bool-only Channel, ResultChannel without
the connection/wait protocol, and nonmatching native transports are rejected
with std::invalid_argument before callback registration or lifecycle actions.

A bool-only false cannot be mapped to an accurate SendRejection. To migrate a
custom channel, implement the six typed ResultChannel admission methods and
the ConnectionChannel capture/cancel/WriteConnection protocol described in
[channel_write_results.md](channel_write_results.md#connection-pinned-custom-reliable-sends).
Reconstructing a reason from flags after calling a bool method is not supported.

The low-level Channel bool interface remains available for direct users and
fanout transports. Its continued existence does not imply compatibility with
the structured client wrapper contract.

## Consumers and remaining scope

The installed-header consumer checks structured client rejection through
ChannelInterface and performs its loopback through contextual bool checks.
Custom/native regression tests also assert the public returned reasons.

Language bindings that promise bool can use static_cast<bool>(result) to
support both the old bool return and the new explicitly convertible result.
The Python repository still pins core v0.9.6; its bindings must be adapted
before upgrading that pin to this API. Exposing rich Python results is a
separate API decision. Server broadcasts now return FanoutResult; see
[server_fanout_results.md](server_fanout_results.md).

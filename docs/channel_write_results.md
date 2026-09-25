# Single-target Channel write results

interface::ResultChannel is an optional single-target admission capability
derived from the existing interface::Channel. TCP clients, UDS clients, UDP
channels and serial transports implement its six async_*_result methods for
copy, move and shared data, with ordinary and explicit try forms.

Each method returns the exact SendResult selected by the existing admission
implementation. It performs no capacity wait. Acceptance is local queue
admission, not delivery. A rejected move keeps its original storage; a result
cannot be revised by a later asynchronous failure.

The inherited final bool methods call the matching typed method exactly once
and return accepted(). Existing Channel consumers and bool-only custom
channels retain their source-level behavior. This changes native class
inheritance, virtual tables and exported symbols, so all consumers must rebuild.
Subclasses of the four native transports that overrode bool writes must instead
override the corresponding typed methods; the inherited bool adapters are final.

## Why a separate capability

Channel also represents servers: a UDS server's generic write fans out to
multiple sessions, while a TCP server's generic write selects its current
session. A single rejection cannot describe partial fanout. Those server
implementations remain on Channel until the separate aggregate contract is
implemented. No representative rejection reason is invented from bool.

## Custom implementations

A custom single-target channel can derive from ResultChannel and implement
the six typed admission methods along with Channel lifecycle and callbacks.
It must choose a truthful reason at the same synchronized decision that
accepts or refuses the write. Do not call a bool-only write and then sample
connection or queue state to guess why it failed.

Ordinary and try methods keep their distinct queue policies. Invalid payload,
lifecycle/readiness, and queue refusal must have explicit outcomes; admission
and stop must have a defined ordering. Do not take ownership on rejection.
The legacy bool adapters are final so typed and bool callers cannot observe
different admission implementations.

```cpp
void inspect(wirestead::interface::Channel& channel,
             wirestead::memory::ConstByteSpan payload) {
  auto* results = dynamic_cast<wirestead::interface::ResultChannel*>(&channel);
  if (!results) return; // Legacy or fanout channel; no structured capability.
  const auto result = results->async_try_write_copy_result(payload);
  if (!result.accepted()) {
    const auto reason = result.reason();
    (void)reason;
  }
}
```

## Connection-pinned custom Reliable sends

Implement interface::ConnectionChannel when a custom channel is injected into
TcpClient, UdsClient, UdpClient or Serial and needs the same Reliable wait
guarantees as the built-in transports. It extends ResultChannel with
capture_write_connection() and cancel_write_waits(). The wrapper discovers
this capability automatically.

Capture returns either the actual readiness rejection or a non-null
shared_ptr<interface::WriteConnection>. Validate readiness and select the
connection in one synchronized decision. The handle owns the old connection
record, not merely a sampled connected flag or a pointer to the channel's
current connection.

The handle exposes:

- poll_capacity(): nullopt while blocked, acceptance when admission may be
  attempted, or the retained terminal rejection. Acceptance reserves no space.
- write_copy, write_move, write_shared: atomically check the pinned
  connection and admit to its queue. Never redirect old work to a new connection.

A channel must serialize capture, polling, cancellation, connection loss,
replacement and final admission. Record NotReady for connection loss and
CancelledWhileWaiting for stop; preserve whichever terminal event happened
first on the old connection record. A new connection gets a fresh record.
Sampling flags only when polling cannot implement this contract: stop and
reconnect can both happen between polls.

The wrapper cancels custom waits before notifying its waiting senders. Direct
Channel::stop must cancel waits too. All capability operations must return
promptly and must not call channel or user callbacks inline: capture,
cancellation and final admission can run under the wrapper mutex. Handles
must remain usable until all waiting sends release them. Returning a null
capture handle is an implementation error and throws std::logic_error.

The wrapper validates payload size, including line delimiters and the reported
hard queue limit, before capture. Callback callers never enter a capacity wait.
It polls with a bounded timeout so a missed notification cannot hang a released
wait, retains a completed wait's rejection, and retries only WouldBlock, at
most five admission attempts on the same handle. Callback admission is attempted
at most once. Explicit try sends call the channel's typed try methods without
polling; BestEffort maps their WouldBlock to QueueFull.

Move storage is retained on rejection and shared storage is not retained for
refused work. Acceptance still means local queue admission, not delivery.

## Remaining wrapper work

ChannelInterface and the four public client wrappers still return bool.
ConnectionChannel now preserves structured decisions internally across the
complete custom send path. The public SendResult migration still needs to
define the compatibility boundary for injected legacy channels: a bool-only
refusal cannot be converted to a truthful SendRejection. ResultChannel alone
does not supply the connection protocol and continues through the legacy
wrapper path. Existing bool-only injected channels keep their prior behavior.

The ResultChannel tests exercise all six forms, all result reasons through a
custom implementation, and actual never-started rejection/counters and
retained move storage across the four native transports. ConnectionChannel
tests cover all four wrappers and three ownership forms, deterministic
loss/stop/restart ordering, replacement after capacity release, bounded retries,
callback refusal, validation, and explicit try/BestEffort policy. Built-in
transport wait/admission implementations are unchanged.

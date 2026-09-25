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

## Remaining wrapper work

This capability does not itself migrate ChannelInterface or the four public
client wrappers: those still return bool. Their native connection-pinned
wait/admission implementation remains unchanged. Custom connection identity,
first-terminal wait causes and final pinned admission need an explicit
contract before exposing the complete wrapper result API. Implementing
ResultChannel alone does not provide that wait/connection protocol.

The ResultChannel tests exercise all six forms, all result reasons through a
custom implementation, and actual never-started rejection/counters and
retained move storage across the four native transports. Existing native
tests continue through the bool adapters and therefore cover the same typed
entry points for live admission.

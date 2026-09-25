# Server fanout admission results

ServerInterface and TcpServer, UdsServer and UdpServer now return
FanoutResult from broadcast, try_broadcast, broadcast_line and
try_broadcast_line. This completes the C++ public fanout part of D-3 and
C-3.6-3/4; it does not implement later discard/event observability or
keep-latest policies.

## Reading a result

All counts concern local queue admission, never delivery.
```cpp
#include <wirestead/wirestead.hpp>

void publish(wirestead::wrapper::ServerInterface& server) {
  const auto result = server.broadcast("update");
  if (result.empty()) {
    // No target sessions were selected.
    return;
  }
  const auto accepted = result.accepted_count();
  const auto rejected = result.rejected_count();
  const auto blocked = result.rejected_count(wirestead::SendRejection::WouldBlock);
  (void)accepted;
  (void)rejected;
  (void)blocked;
}
```

- target_count() equals accepted_count() plus rejected_count().
- rejected_count(reason) counts that reason, including zero when absent.
- No representative reason is chosen when outcomes differ.
- Explicit bool conversion means at least one target accepted. Both zero
  targets and all targets rejected convert to false; use empty() to tell them apart.
- Zero targets always means an empty result, including before start, after
  stop and for invalid payloads when no targets exist. There is no artificial
  target or call-level rejection added to the aggregate.

## Admission rules

Both ordinary and try fanout are nonblocking with respect to queue capacity,
regardless of BestEffort or Reliable. Each target uses try admission, including
WouldBlock for transient pressure; there are no capacity waits or retries.
Ordinary locking and allocation still occur; this is not a wait-free API.

TCP/UDS select the currently live session objects once and retain them through
the call. New connections after selection are excluded. Each admission verifies
that the original session is still mapped to that ID; a lost target is counted
as NotReady, never silently skipped or redirected to a replacement session.
Native shutdown can instead yield its lifecycle rejection. Once accepted, a
target stays accepted in the returned result even if its connection later ends.

UDP holds its virtual-session set stable through the nonblocking traversal.
Each known endpoint contributes one outcome using the shared UDP socket queue,
with the retained native run checked at admission. Consequently queue capacity
can be consumed by earlier targets in the same broadcast; target order is
unspecified for all transports.

Payload validation precedes lifecycle/capacity for each selected target.
Empty raw data yields InvalidArgument; exceeding MAX_BUFFER_SIZE or the
session's whole-queue hard limit yields TooLarge. Line forms include the
newline in the size check before allocating their payload; an empty line
is a valid one-byte request. Early wrapper/validation rejection is not a native
admission attempt and does not increment native failure counters.

## Migration

This is a source/ABI break: rebuild all consumers and update subclasses of
ServerInterface. Contextual checks such as if (server.broadcast(data)) still
work. Change bool assignments or bool-returning adapters to
static_cast<bool>(server.broadcast(data)), or compare accepted_count() with zero.
Check rejected_count() explicitly when partial failure matters.

The native transport bool broadcast APIs remain unchanged. Python bindings
must adopt this aggregate before upgrading their core dependency; their current
v0.9.6 pin and package are not changed here.

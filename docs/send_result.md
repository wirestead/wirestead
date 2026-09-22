# SendResult value type

The D-3 acceptance result is available from
wirestead/wrapper/send_result.hpp in the wirestead::wrapper namespace, or
through wirestead/wirestead.hpp as wirestead::SendResult and
wirestead::SendRejection.

**Current status:** the value type landed in PR #663. Shared wrapper
payload-size validation now uses it internally for InvalidArgument and
TooLarge. Existing send APIs still return bool and do not expose a
SendResult. State/capacity mapping and final admission remain later steps.

## Constructing an outcome

Use SendResult::accept() for an accepted request and
SendResult::reject(reason) for a refusal. There is deliberately no default
constructor and no conversion from bool: a producer must choose an explicit
outcome, including the reason when rejecting it.

accepted() reports local admission only. A true result does not promise a
network write, peer receipt or successful processing. Loss after acceptance
belongs in statistics, and fanout needs its own aggregate result.

## Inspecting an outcome

An explicit bool conversion supports if (result), if (!result) and
static_cast<bool>(result). Copy-initialization into bool is intentionally
rejected; use accepted() when a plain bool is required.

reason() is valid only when accepted() is false. It asserts that precondition
in assertion-enabled builds; calling it on an accepted value violates the
API contract, including when assertions are disabled. It is not a success
code accessor.

All factories and accessors are constexpr and noexcept. Outcomes are
trivially copyable. Their object layout and the enum's numeric values are
not a persistence or wire-format contract.

## Example

This constructs outcomes directly; existing sends still return bool.

```cpp
#include <wirestead/wirestead.hpp>

int main() {
  const auto admitted = wirestead::SendResult::accept();
  if (!admitted) return 1;
  const auto refused = wirestead::SendResult::reject(wirestead::SendRejection::QueueFull);
  if (refused) return 2;
  return refused.reason() == wirestead::SendRejection::QueueFull ? 0 : 3;
}
```

## Reasons

The value type carries all eight reasons from the
[D-3 decision](communication_contract_v0.10_decisions.md):

| Reason | Intended admission meaning |
| --- | --- |
| NotStarted | No active started run |
| Stopping | Shutdown requested and incomplete |
| NotReady | Target or waited-on connection is unavailable |
| WouldBlock | Capacity would require a disallowed wait |
| QueueFull | BestEffort refuses due to capacity |
| TooLarge | Payload exceeds the applicable size limit |
| InvalidArgument | Empty/null or otherwise invalid input |
| CancelledWhileWaiting | Stop released a capacity waiter |

Only the internal payload-size InvalidArgument/TooLarge checks currently use
these mappings; bool-returning transports do not expose the reasons yet. The enum can grow; switches should include
a default branch.

## Validation and migration

- Full Debug build with -j2 passed.
- Full CTest: 989 discovered, 977 passed, 12 existing UDP diagnostic skips,
  zero failures. Ten new tests cover acceptance, copying and every reason.
- Compile-time checks cover constexpr/noexcept use, trivial copying,
  no default construction and no implicit bool conversion.
- Installed direct-header and umbrella-header examples compiled and ran with
  GCC and Clang, with and without NDEBUG. Both compilers rejected implicit
  conversion to bool against the installed public headers.
- The ten focused value tests also passed in NDEBUG mode.
- clang-format, cmake-format and git diff --check passed.

Adding the standalone type does not change existing send signatures or ABI.
The future return-type replacement will require callers such as
bool ok = channel.send(data) to change to accepted() or explicit conversion.
Bindings, per-target reason tests, cancellation/connection fencing and
fanout results remain part of that later migration.

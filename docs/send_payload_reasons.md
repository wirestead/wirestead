# Payload-size rejection reasons

The D-3 value type from PR #663 now participates in the shared validation
used before capacity waiting by all seven wrappers. This is an internal
validation step; public sends still return bool.

## Decision order

The constexpr, noexcept validate_payload_size helper reports:

1. InvalidArgument when the complete payload size is zero.
2. TooLarge when it exceeds MAX_BUFFER_SIZE or a reported whole-queue hard
   limit.
3. Success when those size constraints pass.

The maximum is inclusive. A known zero queue limit rejects nonempty data,
while an unavailable limit imposes no extra restriction. A queue limit
larger than MAX_BUFFER_SIZE never removes the per-message maximum.
Line callers provide the complete size including the newline.

No size arithmetic or allocation is performed, including for SIZE_MAX.
This avoids introducing overflow while classifying oversized inputs.

## Integration boundary

payload_needs_capacity now uses this result's accepted() accessor. Existing
wrapper callers therefore retain the same wait/bypass decisions. Invalid
sizes still reach the existing transport write path for rejection, callbacks
and accounting. Existing early null/empty shared-buffer checks are unchanged.

A successful validation result only passes this stage. It says nothing
about readiness, current capacity, shutdown, connection identity or final
queue acceptance. The new helper is in wrapper::detail and is not a new
public send API.

## Validation

- Full Debug build with -j2 passed.
- Final full CTest: 996 discovered, 984 passed, 12 existing UDP diagnostic
  skips, zero failures.
- Seven new validation tests passed in Debug and with NDEBUG using both GCC
  and Clang. Compile-time checks cover constexpr evaluation and noexcept.
- Existing wrapper integration tests retain their wait, rejection and
  accounting checks across all seven targets.
- clang-format, cmake-format and git diff --check passed.

Tests check empty-input precedence, inclusive message and queue boundaries,
zero/unknown queue limits, maximum size_t and newline size accounting.
Existing integration tests exercise all seven wrappers under pressure,
including continued normal waiting and transport failure accounting.

## Remaining work

Public SendResult returns, synchronized state/capacity rejection reasons,
stable cancellation, connection-instance fencing, fanout results and
bindings remain D-3 work. No existing public signature or ABI changes here.

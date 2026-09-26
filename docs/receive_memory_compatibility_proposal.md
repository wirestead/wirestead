# Receive-memory compatibility proposal

Status: proposal only; public interface changes are not applied or approved.
The overflow, event and configuration policies approved on 2026-09-26 remain
unchanged. The pending question is how that contract applies to external
IFramer implementations.

## Concrete change under consideration

Add the following requirement to IFramer, with implementations in LineFramer,
LengthPrefixFramer and PacketFramer:

    virtual size_t buffered_bytes() const noexcept = 0;

The result describes input payload bytes retained for later framing, including
partial prefixes and delimiters. It must be callable between push_bytes calls;
reset must report zero. A bounded wrapper reserves additional payload allowance
before passing input and reconciles it afterwards. Custom framers must report
their retained input honestly; this is not a sandbox for arbitrary user code.

This method alone does not bound allocator capacity, scratch allocations or
process RSS. The integration must separately account for retained storage and
transient batches, and must not describe a logical payload limit as a process
memory guarantee. UDP admission also needs all-or-nothing handling of the newly
arriving datagram before user callbacks; already retained work must survive.
These remain implementation and verification requirements.

## Source and binary impact

Adding a pure virtual method requires custom framers to implement it. Old
compiled C++ consumers must rebuild against the new headers and library.
A workspace search found a real affected implementation:

- wirestead-examples/examples/tcp/async/telemetry_protocol.hpp:
  telemetry::LengthPrefixedFramer, currently derived directly from IFramer.

Its minimal source migration for the proposed query is:

    size_t buffered_bytes() const noexcept override { return buffer_.size(); }

The examples repository needs a coordinated migration and consumer build.
The Python and documentation repositories contained no direct custom-framer
class in the searched C++/Python source files. That does not prove external
consumers are unaffected.

## Changes avoided in the revised design

MessageContext keeps its current constructors, copy/move behavior and layout.
Reservation ownership belongs to internal queue/batch envelopes and remains
alive through callback completion, including a concurrently retired session.

RuntimeStats keeps its current layout. New receive statistics can be returned
by separate concrete-wrapper methods, implemented through existing Pimpl
storage, rather than inserting a new RuntimeStats member.

The approved overflow actions and event policy do not imply approval of the
public-framer source and binary break described above.

## Compatibility-preserving alternative

Leave IFramer unchanged and provide bounded handling only for known built-in
framers, or introduce an optional capability interface for custom framers.
An unsupported custom framer must be reported as unsupported; silently claiming
a bounded receive-memory guarantee for it is not acceptable. This preserves
the existing interface but leaves an explicit custom-framer coverage exception
unless the application opts into the new capability.

Decision needed: require the new contract for all framers in v0.10, with
coordinated migration/rebuild, or retain compatibility and explicitly limit
coverage for custom framers that do not provide the capability.

## Work already safe to review

The receive-memory worktree contains an internal shared byte/session budget.
It is not connected to transport receive paths, so it does not yet enforce the
approved policy. Tests cover aggregate admission, other-session preservation,
retired scope lifetime, shrink/merge accounting, size overflow, statistics reset
and concurrent reservations. Eight cases passed ASan/UBSan, including 100
repetitions (800 case executions). No claim of full policy conformance follows
from these foundation tests.

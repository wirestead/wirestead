# Blocking admission and accepted queue preservation

## Selected policy

Built-in transports preserve accepted writes under queue pressure with either
strategy. Explicit blocking wrapper sends use that path even under BestEffort.
Ordinary BestEffort wrapper sends still reject the new request with QueueFull;
try sends reject with WouldBlock. Reliable wrapper sends wait for capacity.

Plain native async_write APIs remain prompt admission APIs, not caller-thread
blocking operations. They reserve against the hard limit and return refusal
when it cannot fit. After acceptance they preserve queued/pending work instead
of applying implicit keep-latest trimming. No keep-latest option is introduced.

This changes legacy native BestEffort behavior: accepted requests are no longer
removed to make room for newer ones. Applications depending on freshness rather
than completeness must coalesce unsent values before submitting them.

## Capacity and concurrency

Every plain write reserves hard-limit capacity before posting enqueue work,
including TCP BestEffort copy/move/shared forms. Try admission counts those
in-flight reservations too. Reservation and pending-to-tx transfer use the same
mutex, so a producer cannot treat the transfer's intermediate state as free
space. Completion releases queue bytes without overwriting concurrent producers.

A pressure watermark controls when wrappers wait and where accepted work is
queued. It is not the hard memory limit: a valid plain request larger than the
watermark may still be accepted if it fits the hard limit. Changing limits live
remains outside this policy's fixed-configuration guarantee.

## Retry and termination

All seven wrapper blocking paths retain the original connection/session/run
through every retry. Only WouldBlock is retried, without an attempt limit. A
short condition-variable wait between unsuccessful attempts releases the CPU;
no wrapper/session lock is held during that pause. It is not a send timeout.

Validation and terminal admission failures still return promptly. A wait can
remain unbounded while the target stays ready and capacity remains unavailable.
Stop, connection loss or UDP session expiry ends the retained wait. Callback
callers make at most one admission attempt and never wait or retry.

Acceptance still does not prove delivery. Stop, connection/socket loss and UDP
expiry retain their separately documented post-acceptance outcomes.

## Compatibility and verification

This is a behavioral change for native BestEffort plain writes and callers that
previously relied on a fifth WouldBlock ending a blocking call. No public send
signature changes. Custom WriteConnection implementations must preserve their
accepted requests and provide truthful capacity/terminal results; the wrapper
cannot enforce a custom transport's internal queue policy.

Tests cover preservation under both strategies, all native input families,
mixed reservation hard limits, retries beyond five using one pin, terminal
release, move ownership and callback refusal.

Local validation: 2,586 discovered tests (2,574 passed and 12 existing UDP
large-payload skips), 961 related AddressSanitizer cases, and 74 boundary cases
repeated 100 times under AddressSanitizer (7,400 passes). An installed shared
library consumer and direct TCP/UDS session layout/accounting smoke also pass.
Platform CI results are recorded on the pull request.

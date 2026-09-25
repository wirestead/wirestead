# TCP wrapper nonblocking send results

> Historical implementation-stage record. Public return types and custom-channel
> compatibility have since changed; see [the current client API](client_send_results.md).

Built-in TCP wrapper try_send, try_send_line, try_send_move and try_send_shared
now retain an internal SendResult through their bool boundary. Ordinary send,
send_line, send_move and send_shared use the same path under BestEffort.

## Decision order

1. Validate shape, MAX_BUFFER_SIZE and the available native whole-queue limit.
   Empty/null input produces InvalidArgument; oversize produces TooLarge.
   Line methods include their newline, so an empty line is a valid byte.
2. Observe wrapper lifecycle. A running stop call produces Stopping. After a
   request-only stop, active callbacks or pending native cleanup still produce
   Stopping; completion produces NotStarted. A native transport connected
   independently of its wrapper does not bypass the wrapper start requirement.
3. Let the native helper decide current state and capacity together under its
   submission mutex. This preserves the actual admission outcome.
4. Preserve WouldBlock for explicit try methods. Map native capacity refusal to
   QueueFull only for ordinary BestEffort sends.

Validation happens before transport handoff. These early failures do not
increment native failed-send or dropped-message counters; that is an observable
accounting change. A disconnected/unstarted wrapper already bypassed native
handoff for valid payloads. Native outcomes after handoff retain their existing
accounting and rejected move inputs retain their storage.

The wrapper stop-call count covers shutdown before native stop publication and
through outside-caller finalization. After an executor-local request-only return,
the callback gate and the native cleanup completion signal determine whether
stopping work remains. This is independent of the link's Closed state.

## Compatibility and scope

Public signatures still return bool. Custom Channel implementations retain
their existing bool behavior, including validation and accounting delegation.
Their bool refusal is not assigned an invented typed reason. The internal
observer hook runs after releasing the wrapper mutex and only reports the
built-in/native path.

Reliable and explicit blocking sends now also combine wrapper entry checks,
retained wait outcomes and native admission; see
[tcp_reliable_send_results.md](tcp_reliable_send_results.md).
Public SendResult migration must account for the common ChannelInterface
and other implementations, and remains separate work.

## Regression coverage

Twelve real TCP cases cover copy, line, move and shared forms for explicit try
under both strategies and ordinary BestEffort sends. They check validation before state, the line
delimiter at the hard limit, pre-start, connecting, queue refusal, executor-local
stop, completion and restart. Two more cases cover independently connected native
injection before wrapper start and null shared buffers before/after stop.

## Validation

- Full Debug build and final CTest with -j2: 1145 discovered, 1133 passed,
  12 existing UDP diagnostic skips, zero failures.
- ASan with leak detection: 195 selected TCP cases passed before adding the
  explicit-try/BestEffort matrix extension; all final 14 result cases then passed.
- TLS-enabled build: 96 selected cases passed, followed by all final 14 result
  cases after the matrix extension.
- clang-format and git diff --check passed.

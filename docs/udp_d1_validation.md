# UDP D-1 validation

This change follows UDS PR #654, merged as
e1594fb18b9423667b98f0a91a52a3b16c1d3d45 after all 43 CI checks passed.
It covers the common UDP transport and the UDP client/server wrappers.

## Contract and implementation

Every outside stop caller waits for actual transport cleanup and an idle
wrapper callback gate. Calls on the target executor or inside the target's
callbacks request shutdown and return; calls from an unrelated executor wait.
External contexts must continue progressing. Stop never polls the context,
uses a timeout fallback or interprets a stopped context as cleanup completion.

Write submission is serialized with the cleanup post. Cancelled receive/send
handlers are counted until their work and pooled-buffer release finish.
Restart restores the configured destination, discards a learned peer and
rejects work from the previous generation. Writes before start reject without
retaining executor work. Oversized move/shared write rejection still returns
false; its error callback now runs on the strand, ordered with cleanup.

Wrapper gates cover callback admission and postprocessing, including batch
timers and the server's peer-expiry timer. A delayed handler holding a liveness
reference still cannot enter a later generation. Outside finalization resets
timers, batches and framers/sessions after admitted callbacks finish, then
joins managed threads. Injected transports retain their identity and receive
fresh handlers on restart.

## Regression tests

The 30 new tests cover:

- Two outside callers waiting for an active callback and actual cleanup.
- Callback-initiated stop, subsequent outside completion, then real data
  receipt on the same injected transport from a newly learned peer.
- Client/server wrappers with owned and externally run contexts; external
  cases use two runners.
- Target-executor request-only stop versus unrelated-executor waiting.
- Batch-timer callback stop and server expiry callback stop.
- A saved client handler and a server reaper parked before admission across
  stop/restart; the old generation is refused.
- Final cancelled I/O completion, no unrelated executor pumping, never-started
  and failed-start shutdown, pre-start write rejection and reentrant stop.
- Oversized-write error callbacks safely requesting stop.

Existing manual-context tests now keep their executor running during outside
stop. Existing peer filtering, datagram, backpressure and send-contract
assertions are unchanged.

## Before/after evidence

The existing comparison worktree is at 757bd87e3. Its UDP transport and both
UDP wrapper sources were byte-compared against merged main e1594fb18 and
are identical; this makes no claim about other transports in that worktree.
Only the new public-API loopback test file and its build registration were
copied there. No UDP implementation fix or UDP stop instrumentation was copied
into the comparison library.

Both external-context cases of ConcurrentOutsideStopsWaitForCallback
(client parameter 1, server parameter 3) fail on that unchanged implementation:
both outside stop callers return while the callback is still active.
The same cases pass on this branch. Source equality hashes and comparison
logs are retained in the workspace's log/codex-udp-d1-validation directory.

## Validation results

- GCC Debug, TLS off: full build with cmake --build build -j2.
  ctest --test-dir build --output-on-failure --timeout 60 -j2 discovers
  893 tests: 881 pass, 12 existing UDP large-payload tests skip, zero failures.
- AddressSanitizer + leak detection: WIRESTEAD_ENABLE_SANITIZERS=ON and
  WIRESTEAD_ENABLE_ASAN=ON, with libasan.so.8 linkage verified. With
  ASAN_OPTIONS=detect_leaks=1:halt_on_error=1, all 49 selected tests pass:
  30 new D-1 cases, 16 existing UDP transport cases and 3 UDP send-error cases.
  No sanitizer or leak reports were emitted.
- git diff --check and clang-format checks pass.
- Validation ran under Ubuntu 24.04 in WSL from the Windows desktop host.
  The new PR's remote CI is a separate validation stage.

## Limits

Restart is allowed only after every outstanding stop call finishes.
Concurrent start/stop and destruction from a callback remain unsupported.
Stop cancels; it does not guarantee flushing queued bytes or peer delivery.
External executors must keep running while an outside caller waits.
TSAN, performance claims, serial and D-2/D-3 are outside this validation.

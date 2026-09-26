# UDP callback serialization

Built-in UDP clients and servers now use the socket strand for wrapper batch
timers and the server session reaper. UdpChannel::get_executor returns that
strand, rather than the raw io_context executor. Work posted through this API
therefore serializes with native I/O and wrapper timer callbacks.

Previously, a second io_context runner could flush a batch or expire a session
while the receive callback was still executing. The same client or virtual
session could then have overlapping user callbacks. Sharing the socket strand
closes that scheduling gap without a callback mutex held across user code.

## Scope and timing

One UDP socket has one strand. Its peer sessions share this execution path;
this change does not promise parallel callbacks for different peers. Other
channels can still use other executors or strands. Tasks explicitly posted to
a caller-owned raw io_context remain outside the strand. Stop from that same
I/O context is still request-only; external stop waits for strand progress and
callback completion, including a reaper parked before callback admission.

A long-running callback delays batch delivery and session reaping on that socket.
Batch latency and the idle timer schedule work; they do not guarantee callback
or cleanup deadlines when the executor or strand cannot progress. The reaper
evaluates the current last-seen state when it runs. Existing stop completion,
callback refusal to wait, and generation fencing remain in place.

The selected expiry policy is unchanged: discard waiting accepted requests,
retain active completion outcomes, and preserve other peers' requests. Expiry
still uses the existing callback; a distinct expiry event remains undecided.
The executor change neither assigns mixed-peer batches to a new public scope
nor resolves TCP/UDS connect ordering or general executor-thread blocking sends.

## Verification

Two controlled io_context runners exercise client/server data and message
batch timers while a receive callback is held. A separate case holds a server
receive callback while the expiry timer becomes ready. All five cases fail
with the previous raw executor and pass with the shared strand.

Expiry accounting tests defer only the I/O completion, leaving the strand free
to process timers. They still verify active-versus-waiting work, queued and
pending storage, expiry before stop, other peers, and endpoint reuse. The
internal completion seam is unset in normal operation; tests resume each held
completion on the channel executor before waiting for shutdown.

Local validation: 2,591 discovered tests (2,579 passed, 12 existing UDP
large-payload skips), 292 UDP AddressSanitizer cases, and 15 scheduling/expiry/
stop boundary cases repeated 100 times under AddressSanitizer (1,500 passes).
Installed shared-library consumer and direct session layout/accounting smoke
checks pass. Platform CI results are recorded on the pull request.

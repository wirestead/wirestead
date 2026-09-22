# Changelog

All notable changes to Wirestead are documented in this file.

This project follows the Keep a Changelog section names where practical. The
core C++ API is still pre-1.0; see `docs/api_stability.md` for compatibility
and ABI policy.

## Unreleased

### Added

- Built-in TCP capacity waits retain their first stop/loss reason in an
  internal SendResult: CancelledWhileWaiting for stop, NotReady for connection
  loss. A selected capacity-release result also remains fixed. Public send
  methods still return bool while the D-3 migration continues.

- Shared wrapper payload-size validation now produces SendResult internally:
  InvalidArgument for empty input and TooLarge for message/queue size limits.
  Public sends still return bool; transport rejection and accounting remain
  unchanged while D-3 migration continues.

- SendResult and SendRejection provide the D-3 acceptance-result value type:
  explicit accept/reject factories, accepted(), an explicit bool conversion,
  and a reason() accessor for rejected values. Existing send APIs still return
  bool; this is the result-type foundation, not the transport migration.

### Changed

- **Breaking behavior:** TCP reconnect no longer replays data accepted on the
  previous connection. Posted writes, queued/pending buffers and unfinished
  write batches are discarded and counted as dropped. Late completions retain
  their own buffers and cannot alter the replacement connection.

- **Breaking behavior:** TCP client writes now reject before a usable connection
  exists, including before start, while connecting/TLS handshaking and after
  connection loss. Readiness publication shares the write-admission mutex.
  Callers that previously queued data offline must wait for connection readiness.

- **Breaking ABI:** Channel gains a default virtual write_queue_limit() query, and
  concrete transports report their admission limit. Custom Channel subclasses
  remain source-compatible, but libraries and consumers must be rebuilt.

- **Breaking behavior:** Blocking-capable sends never wait for capacity inside
  any wrapper user callback, including connect, disconnect, error,
  backpressure and timer-delivered batches. They return false under pressure;
  with capacity available, normal acceptance still applies, including sends
  to another channel. Outside-callback waiting and try-send behavior are
  unchanged.

- **Breaking behavior:** TCP client and server outside `stop()` callers,
  including concurrent callers, wait for transport cleanup and for running
  callbacks to finish. Server completion includes each live session's cleanup.
  A call on the target executor requests shutdown and returns without waiting;
  a call from an independent executor still waits. External executors must
  keep running during that wait.
- TCP restart opens a new callback generation atomically. Delayed callbacks,
  accepts and retries from the previous run cannot enter the restarted run.
  The stopping thread does not poll a shared executor or use a timeout as
  evidence that cleanup is safe.
- D-1 also applies to UDS, UDP client/server and serial in the follow-ups
  below. D-2 follows in this release; structured send results (D-3) remain
  separate work.
- **Breaking behavior:** UDP client/server outside `stop()` callers now wait for
  every admitted callback and transport cleanup, including cancelled I/O.
  Calls on the target executor request shutdown and return; external contexts
  must continue running until outside stop completes. Before-start writes
  reject without retaining work; restart discards a learned peer while keeping
  the configured destination.

- **Breaking behavior:** Serial outside stop callers, including concurrent
  callers and callers on unrelated executors, now wait for transport cleanup
  and admitted callbacks on owned, external and shared contexts. Calls on the
  target executor request shutdown and return; external contexts must keep
  running while outside callers wait. Before-start writes reject without
  retaining executor work.
- Serial tracks cancellation through read/write, retry and receive-idle timer
  completion, retains active gather-write buffers until release, and refuses
  previous-run wrapper callbacks and batches after restart. Injected channels
  retain their identity and regain handlers on restart.

### Fixed

- Blocking/Reliable sends using the built-in TCP client now pin the connection
  across capacity waits and retries. A reconnect ends the old wait, and the
  expected connection is checked under the transport admission mutex so a
  replacement connection cannot accept the old payload.

- TCP blocking/Reliable sends now pin their wrapper run across capacity waits
  and retries. A stop followed by restart cannot resume an old send in the new
  run, even if the replacement run is also under backpressure.

- TCP client write admission now serializes state checks, queue reservation and
  strand submission with explicit stop requests. A checked write cannot be
  admitted after stop cleanup. Executor-origin writes now post their routing
  work so callbacks can request stop without reentering the admission mutex.

- UDP server Reliable sends and explicit blocking sends use ordinary write
  admission after waiting, accepting payloads above the pressure watermark
  when they fit the hard queue limit. Explicit try-send and BestEffort sends
  retain their nonblocking watermark limit.

- Blocking-capable sends on all seven wrappers bypass capacity waits when a
  payload exceeds the transport's reported whole-queue hard limit. Existing
  transport rejection and accounting are retained.

- TCP blocking sends now check connection readiness before waiting and before
  submitting a copied payload. A disconnected channel with stale pressure no
  longer holds the caller until that pressure clears.
- UDP server blocking sends to an unknown client ID no longer wait for the
  shared channel's queue pressure to clear.

- Reliable and explicit blocking sends no longer wait for queue capacity
  before rejecting an empty payload or a payload above MAX_BUFFER_SIZE.
  This applies to all seven wrappers, including newline-appending calls.
  Transport rejection, error callbacks and failure accounting remain in
  place; valid payloads retain their existing waiting behavior.

- UDS server session backpressure transitions now reach the registered server
  callback. Handler snapshots allow replacement after connection and callbacks
  run outside the session-map mutex.

- UDP restart refuses old callback and timer generations, including batch
  delivery and server peer expiry. Injected transports retain their identity
  and regain callbacks on restart. Pending writes and pooled buffers finish
  cleanup before a new run; oversized-write error callbacks are dispatched
  on the transport strand so they can safely request stop.

- UDS client/server outside `stop()` callers now wait for transport cleanup
  and admitted callbacks; target-executor calls request shutdown without
  blocking. Restart rejects previous-run callbacks, client cancellation retains
  active I/O storage, and server cleanup waits for every session strand.
  Never-started and failed-start servers stop without requiring an executor;
  socket-path ownership protections remain in place.

- `stop()` called from inside a serial callback threw instead of stopping.

  The callback runs on the transport's own io thread, and `stop()` joined that
  thread unconditionally, so the call joined the thread with itself:
  `std::system_error`, "Resource deadlock avoided". Without a catch at the call
  site the library's callback dispatch swallowed and logged it, which hid the
  real damage - the throw unwound before `io_context::restart()` and before the
  wrapper released the channel, so the object was left half-stopped. A later
  `stop()` returned, but a restart's future never completed.

  A `stop()` from the io thread now requests the shutdown and returns without
  joining, and a later `stop()` from outside completes it, waiting even when a
  shutdown was already requested. After that call returns the object can be
  restarted and destroyed. Restarting from inside a callback, before the
  shutdown completes, remains unsupported.

  Reproduced in `test/repro/serial_stop_in_callback_repro.cc`; covered by
  `SerialStopInCallbackTest`, which checks that the callback's `stop()` does
  not throw and that a restarted channel receives data again.

- The CPack Debian package named the wrong Boost package.

  It required only `libboost-system-dev`. The package ships the headers as well
  as the shared library, and those headers reach `boost/asio`, so it also needs
  `libboost-dev`. `libboost-system-dev` stays, because `wiresteadConfig.cmake`
  calls `find_dependency(Boost COMPONENTS system)` and that needs the
  component's CMake files. These are the same two keys `package.xml` has
  declared since v0.9.6; the CPack side was missed then because only the ROS
  packaging was in view.

  A CPack package has never been published, so nothing installed is affected.

### Removed

- **Breaking:** the Unilink compatibility layer, promised for the v0.9.x line
  only. This is why the next release is v0.10.0 rather than a v0.9 patch.

  Gone: `namespace unilink`, the `<unilink/...>` forwarding headers,
  `find_package(unilink)` with its `unilink::unilink`, `unilink_shared` and
  `unilink_static` targets, `unilink.pc`, the `UNILINK_API` / `UNILINK_EXPORT` /
  `UNILINK_LOG_*` macros, `UnilinkException`, and the `UNILINK_*` CMake option
  and `UNILINK_LOG_LEVEL` fallbacks. The first group fails loudly at configure or
  compile time. The last two do not: an old `-DUNILINK_BUILD_TESTS=ON` draws
  only CMake's unused-variable warning, and `UNILINK_LOG_LEVEL=debug` is
  ignored outright.

  Migrate on v0.9.x first, where both names build; see
  `docs/migration-from-unilink.md`. For packagers, the install tree no longer
  contains `include/unilink/`, `lib/cmake/unilink/` or `unilink.pc`, so the
  vcpkg port's `vcpkg_cmake_config_fixup(PACKAGE_NAME unilink)` must go with
  this release or the port build fails.

- `wirestead/memory/memory_validator.hpp`, in full.

  The header declared eleven free functions in `memory::memory_validator`, the
  `MemoryValidator` RAII class and the three `MemoryPatternGenerator` statics.
  **None of them had a definition anywhere** - there is no `memory_validator.cc`
  and never was - so `nm` finds no matching symbol in any library this project
  has ever built, shared or static. Nothing included the header either, not even
  `wirestead.hpp`, yet it was listed in `WiresteadSources.cmake` and so installed
  into the consumer's include directory.

  Nothing can break, because nothing could ever have linked against it. Most of
  the API was also unimplementable as declared: `memory_accessible()` cannot be
  answered portably for an arbitrary pointer, and `double_free()` /
  `use_after_free()` take a raw pointer with no allocator context. The parts that
  were implementable already exist as `base::safe_memory::safe_memcpy` and are
  used by five transports, and the real checking is done by the ASan/UBSan
  Memory Safety Tests job.

- `ThreadSafeState`, `ThreadSafeCounter`, `ThreadSafeFlag` and the
  `ThreadSafeLinkState` alias, from `wirestead/concurrency/thread_safe_state.hpp`.

  **This is a breaking change.** These are templates and inline functions, so an
  external consumer could have been using them successfully; nothing inside this
  project was. There were no instantiations in the library, the tests, or any of
  the six satellite repositories.

  `AtomicState` and its `AtomicLinkState` alias stay, and the header stays with
  them: that alias is the state primitive every transport actually uses. It is
  what made the three removed classes look load-bearing from a distance and they
  are not - `ThreadSafeState` was only ever reachable through
  `ThreadSafeLinkState`, which nothing named.

  Adopting rather than deleting was considered and rejected.
  `ThreadSafeState::notify_callbacks()` took a mutex on every state transition to
  iterate a callback list nothing ever registered into, so converting the
  transports to it would have added a lock per connection state change and bought
  nothing over the `AtomicLinkState` they already use. Removing the three also
  drops `<shared_mutex>`, `<condition_variable>`, `<functional>`, `<vector>`,
  `<mutex>`, `<algorithm>` and `<chrono>` from a header six transports include.

  `wirestead-docs` documents all four types. Those sections describe v0.9.6
  correctly and must be corrected when this change ships: the `ThreadSafeState`,
  `ThreadSafeCounter` and `ThreadSafeFlag` sections of
  `docs/contributor/architecture/memory_safety.md` go, the `AtomicState` section
  stays, and the thread-safety row of its safety-feature table needs rewording.

- `ErrorStats::successful_retries` and `ErrorStats::failed_retries`.

  Both were declared and cleared in `reset()`, and neither was ever incremented
  or read. `retryable_errors` beside them is incremented and stays. Because
  `ErrorHandler::error_stats()` is public, a caller reading these two always got
  0 - not an absent statistic but a silently wrong one.

  Populating them is not a small wiring job: `ErrorHandler` only ever sees
  errors that were reported to it, and a retry that succeeds reports nothing, so
  the success count can never reach it without new plumbing from the reconnect
  path. `RuntimeStats`, where transport telemetry belongs, has no retry counters
  either. Retry telemetry is a feature to design there, not two fields pinned at
  zero here.

- Public API that nothing called, anywhere in this project, its tests, its six
  satellite repositories or its documentation:

  - `GlobalMemoryPool::create_optimized()` and `create_size_optimized()`. The
    per-channel pool path they were meant to serve is used - `serial.cc`
    constructs `MemoryPool pool_{0, 200}` directly - so the factories were a
    redundant second entrance with numbers nobody chose.
  - `PlatformInfo::get_feature_level()` and `get_platform_description()`. The
    other five members of that class stay.
  - `ConfigFactory::create_from_file()` and `get_singleton()`, with the
    singleton storage and mutex they were the only users of.
  - Thirteen `InputValidator::validate_*` overloads: the throwing wrappers for
    host, IPv4, IPv6, UDS path, device path, baud rate, data bits, stop bits,
    parity, buffer size, memory alignment, timeout and retry interval. No
    production code called any of them; only their own unit tests did, which is
    the test existing because the API does rather than a use of it. The six
    `validate_*` that builders actually throw from stay, as do all six
    `is_valid_*` predicates the configs call.

    The tests were not deleted with them. Those parameterized tables were the
    only coverage the `is_valid_*` predicates had, reaching them through the
    wrappers, so they now drive the predicates directly and the tables are
    unchanged. `MAX_DEVICE_PATH_LENGTH` went too - `validate_device_path` was
    its only reader.

- Fifteen constants in `base/constants.hpp` that nothing referenced, including
  `constants.hpp` itself: the whole thread-pool group (there is no thread pool),
  the memory-pool sizing group (`MemoryPool`'s constructor uses its own
  defaults), the cleanup and health-check intervals, the error-recency pair -
  duplicated by the private `ErrorHandler::MAX_RECENT_ERRORS` that is actually
  used - plus `DEFAULT_BUFFER_SIZE`.

- `AsyncLogConfig::batch_size` and `AsyncLogConfig::enable_batch_processing`,
  and the three-argument `AsyncLogConfig` constructor that took a batch size.

  Neither field was ever read. Setting `batch_size` did nothing and said
  nothing, which is worse than not offering it, and the field sat in the middle
  of a public constructor's parameter list where a caller had to pass something
  for it. `enable_batch_processing` was referenced nowhere at all. spdlog
  manages its own batching behind the async logger, so there was never anything
  for either to control.

  The constructor had no callers anywhere in the repository. Anyone who used it
  can construct the struct and assign the two fields that remain.

- The CPack RPM generator.

  `packaging/wirestead.spec` is the supported RPM path as of that file landing.
  It splits runtime from `-devel` the way an RPM distribution expects, and it is
  built by the distribution's own toolchain. A CPack RPM is a single combined
  package built by whatever host runs `cpack`, so one produced on Ubuntu carries
  Ubuntu's glibc dependencies and will not install on an RPM distribution.

  Nothing ran it: `release.yml` passes `TGZ` on every Linux row, so the RPM
  settings had never executed, which is how they came to require `boost-system`
  - a package with no headers - without anyone noticing. Two RPM paths that
  disagree is the condition that let that survive.

### Changed

- The minimum supported spdlog version is now **1.8**, down from 1.9.

  1.9 was never an API floor. It is where `spdlog::sinks::callback_sink`
  arrived, which is the likely origin of the number, but this library derives
  its own callback sink from `spdlog::sinks::base_sink` and has done since that
  code was written. Every other spdlog name it uses predates 1.1.

  Nothing under `wirestead/` compiles conditionally on `SPDLOG_VERSION`, so an
  older spdlog cannot quietly remove a feature and leave a passing build behind.
  `.github/workflows/spdlog-floor.yml` builds and runs the unit suite against
  1.8.2 on Ubuntu 22.04 amd64 and arm64 and on CentOS Stream 9, and is the thing
  that keeps the floor true from here.

  Lowering a floor cannot break an existing consumer - anyone who satisfied 1.9
  satisfies 1.8. What it opens is platforms whose vendored spdlog sits below
  1.9, which includes RPM-based embedded targets.

## v0.9.6 - 2026-08-30

### Added

- `backpressure_threshold()` and `backpressure_strategy()` on `Serial`,
  `TcpClient`, `TcpServer`, `UdpClient`, `UdpServer`, `UdsClient`, and
  `UdsServer`.

  ```cpp
  server.backpressure_threshold(8192);
  const size_t configured = server.backpressure_threshold();  // 8192
  ```

  Both settings were write-only. The setters have been there all along, but
  nothing read them back, so a caller could not log the configuration it was
  running under, round-trip it, or assert on it from a test without tracking
  the value separately alongside the transport.

  v0.9.4 added `RuntimeStats` because backpressure could be configured with no
  way to observe what it produced. That covered the runtime half; the setting
  itself stayed unreadable. This covers the other half.

  The Python bindings raised `AttributeError: backpressure_threshold is
  write-only` on read for the same reason, and can now expose real properties.

### Fixed

- `set_io_thread_init()` is reachable from the umbrella header.

  `wirestead/wirestead.hpp` did not pull in the header that declares it, so the
  documented way to install a thread-init hook only compiled if you also
  included the transport header directly. Anyone following `docs/tuning.md`
  with just the umbrella include got an undeclared-identifier error.

### Packaging

- `package.xml` is now installed to `share/wirestead/`.

  Without it the Debian carried no package manifest at all - no machine-readable
  license, maintainer or dependency metadata at the path the ROS ecosystem looks
  for it. Nothing else changes: the library, headers and `wiresteadConfig.cmake`
  were always installed, and `find_package(wirestead)` behaved correctly the
  whole time.

  This matches what released `build_type: cmake` packages do; every one of the
  eight in Jazzy ships `share/<pkg>/package.xml`. It does **not** make the
  package visible to `ros2 pkg list` or `ros2 pkg prefix`, which read an ament
  index marker that plain CMake packages generally do not install -
  `foonathan_memory_vendor` behaves the same way.

- Boost is declared as a build-time dependency only, and narrowed to the parts
  actually used.

  `package.xml` declared `<depend>boost</depend>`, which resolves to
  `libboost-all-dev` and, because `<depend>` covers exec as well as build, put
  it in the runtime `Depends:` of the generated Debian. `libwirestead.so` has no
  Boost `NEEDED` entry at all - Asio and System are header-only since 1.69 - so
  this was 383 MB of `-dev` packages on every machine that installed Wirestead.
  It is now `libboost-dev` and `libboost-system-dev` as `build_depend` and
  `build_export_depend`, which is 139 MB at build time and nothing at runtime.

  Only Debian packaging is affected. Source, vcpkg and Conan builds resolve
  Boost through their own mechanisms and are unchanged.

### Compatibility

- **Release this rather than v0.9.5 if you are packaging Wirestead for ROS 2.**
  Both packaging fixes above landed after v0.9.5, so a Bloom release from that
  tag produces a Debian that ROS tooling cannot discover and that drags
  `libboost-all-dev` onto every installation.

- No source-level breaking changes. Everything added is a getter alongside an
  existing setter, and the umbrella-header fix only makes a documented API
  reachable where it previously failed to compile.


## v0.9.5 - 2026-08-16

### Changed

- The minimum supported Boost version is now **1.74**, down from 1.83.

  1.83 was never an API floor - it is what Ubuntu 24.04 ships through apt, and
  the number had hardened into a requirement. The oldest Boost.Asio API this
  library uses is `any_io_executor`, which is 1.74. The C++20 concern behind the
  higher number does not apply either: every C++20 entry in Asio's changelog
  between 1.75 and 1.83 is about coroutines, which Wirestead does not use, and
  `std::span` never reaches Asio because every call site converts to pointer and
  size explicitly.

  This is measured rather than argued. `.github/workflows/boost-floor.yml`
  builds and runs the unit suite against Boost 1.74 on Ubuntu 22.04 and Boost
  1.75 on CentOS Stream 9, and is a required job so the floor stays true.

  Nothing breaks by lowering a floor - anyone who satisfied 1.83 satisfies 1.74.
  What it opens is two platforms that were shut out: Ubuntu 22.04, and so ROS 2
  Humble, and RHEL 9, which the ROS build farm compiles for.

- `WIRESTEAD_BUILD_TESTS` now defaults to **OFF**.

  Building tests pulls GoogleTest through `FetchContent`, which clones at
  configure time, so the old default meant a build that never asked for tests
  still needed the network. Packaging environments all passed `OFF` explicitly
  already - vcpkg, the Conan recipe, the ROS integration CI - but Bloom
  generates its Debian rules from `package.xml` without knowing the option
  exists, and would have failed offline on the ROS build farm.

  If you build from source and want the tests, pass `-DWIRESTEAD_BUILD_TESTS=ON`.
  Every preset in `CMakePresets.json` and `scripts/verify.sh` already does.

### Compatibility

- **Release this rather than v0.9.4 if you are packaging Wirestead for a
  distribution.** v0.9.4 shipped before both changes above, so a build from that
  tag still requires Boost 1.83 and still fetches GoogleTest at configure time.
  A ROS build farm, which builds offline against Ubuntu 22.04's Boost 1.74,
  fails on both counts. vcpkg, Conan and PyPI are unaffected, since each of them
  supplies a recent Boost and passes `WIRESTEAD_BUILD_TESTS=OFF` explicitly.

- **A patch release that changes one default.** `docs/api_stability.md` says
  patch releases *should* avoid that, and `WIRESTEAD_BUILD_TESTS` moving from ON
  to OFF is the exception, named here rather than left to be discovered. It
  changes what a plain `cmake` build produces, not what the library does at
  runtime.

- Lowering the Boost minimum cannot break an existing consumer: anyone who
  satisfied 1.83 satisfies 1.74. The generated `wiresteadConfig.cmake`, the
  Debian dependency string and the RPM one all now read 1.74.0.

- No public API changed and no symbol was removed.

- Consumers must rebuild, as for any pre-1.0 release; see
  `docs/api_stability.md`.

## v0.9.4 - 2026-08-16

### Added

- `RuntimeStats::last_receive_age_ms`, milliseconds since bytes last arrived,
  on every transport.

  ```cpp
  auto s = channel->stats();
  if (s.last_receive_age_ms && *s.last_receive_age_ms > 500) { /* sensor is quiet */ }
  ```

  The signal for "has this sensor gone quiet". A device that stops streaming
  without erroring leaves every other counter healthy and the link reporting
  Connected, because nothing went wrong - the data simply stopped. Only the age
  says so. `nullopt` until something arrives, and cleared by `reset_stats()`, so
  a restarted channel cannot report an age carried over from its previous life.

  Passive by design: what to do about silence differs per device, so this
  reports rather than acts. Serial additionally has `rx_idle_timeout`, which
  does act, because there a concrete recovery exists - reopening the port. UDP
  and the socket transports have no such recovery, which is why they get the
  signal rather than a policy.

- `LengthPrefixFramer`, and `use_length_prefix_framer()` on every builder.

  ```cpp
  auto client = wirestead::tcp_client("127.0.0.1", 9000)
                    .use_length_prefix_framer(4)   // 4-byte big-endian length
                    .on_message(...)
                    .build();
  ```

  The layout most binary protocols use, and the one `PacketFramer` cannot
  carry: because the length says how many bytes to collect, no byte value is
  special and the payload may contain anything. Until now the only answer was
  to implement `IFramer` yourself, which the `PacketFramer` warning added last
  release pointed at without providing.

  Prefix width 1, 2 or 4 bytes; big or little endian; and the declared length
  may count the prefix itself or not, since both conventions exist and picking
  wrong shifts every frame by the prefix width.

  A declared length above `max_length` is treated as corruption - the buffer is
  dropped and framing restarts, rather than allocating whatever a bad or
  hostile header asked for.

  It requires the stream to start on a frame boundary, which is the normal case
  for TCP and UDS. There is no sync word, so it cannot resynchronise: a serial
  line you attach to mid-stream, or a protocol carrying a sync word, still
  needs a custom `IFramer`.

- Serial RS-485 and modem control lines: `rs485()`, `dtr()`, `rts()`.

  ```cpp
  auto bus = wirestead::serial("/dev/ttyUSB0", 1000000).rs485().build();
  ```

  Half-duplex RS-485 is what Dynamixel servos, Modbus RTU and much industrial
  sensing run on, and the transceiver's direction pin has to be driven around
  each write at an instant userspace cannot hit - so the kernel driver does it
  (`TIOCSRS485`). Nothing previously exposed this, or the port's native handle,
  so those devices could not be driven at all.

  RTS polarity, full-duplex mode and the pre/post delays are all settable; the
  delays default to 0, which suits most transceivers, and exist because slow
  ones need a millisecond or two that no default can guess.

  A refusal is logged at warning level rather than debug: an adapter that
  switches direction in hardware refuses legitimately, but so does a plain
  UART, and a shared bus running in plain UART mode collides on every write.

  `dtr()`/`rts()` are engaged only when called - leaving one unset keeps the
  driver's default, which differs from passing `false`, since an Arduino
  reboots on an asserted DTR at open.

- UDP multicast receive: `multicast_group(group, interface_address = "")` on
  both UDP builders joins the group after binding.

  ```cpp
  auto lidar = wirestead::udp_client()
                   .local_port(2368)
                   .reuse_address()
                   .multicast_group("239.255.42.99", "192.168.1.10")
                   .on_data(...)
                   .build();
  ```

  Sensors that publish to a group could not be received at all before this;
  `enable_broadcast` covers a different thing. The interface argument names the
  NIC by its local IPv4 address, which a robot with a sensor network on one
  interface and everything else on another needs - leave it empty only on a
  host with one interface. IPv6 groups are supported, always on the kernel's
  choice of interface.

  A failed join fails `start()` rather than coming up as a socket that receives
  nothing: silently not joining is indistinguishable from a sensor that stopped
  sending, which is the confusion the feature exists to remove. A group outside
  224.0.0.0/4 or ff00::/8 is rejected by config validation instead, where the
  error can name the setting.

  Receiving only. Sending to a group already worked through `remote_address`,
  at the default TTL of 1 - one hop, so the local subnet, which is where a
  robot's sensors are.

- `wirestead::concurrency::set_io_thread_init()`, a callback run on every io
  thread the library starts, before that thread services any work.

  ```cpp
  wirestead::concurrency::set_io_thread_init([] {
    pthread_setname_np(pthread_self(), "wirestead-io");
    sched_param p{.sched_priority = 20};
    pthread_setschedparam(pthread_self(), SCHED_FIFO, &p);
  });
  ```

  The library creates its own threads, so a deployment needing a real-time
  policy, a CPU affinity mask, or a readable thread name had nothing to apply
  it to. The hook runs on the new thread, which is what those APIs require.

  Process-wide rather than per channel: thread policy is a property of the
  deployment, and a per-config field would have to be threaded through six
  transports to say the same thing. Install it before starting any channel; it
  does not reach threads already running. Exceptions escaping the hook are
  caught, since a thread entry point would otherwise terminate the process.
  See `docs/tuning.md`.

- Optional TLS for the TCP **client**, behind the same `-DWIRESTEAD_ENABLE_TLS=ON`.
  Two wirestead services can now talk to each other encrypted; before this a
  wirestead client could only reach a wirestead TLS server by not being a
  wirestead client.

  ```cpp
  auto client = wirestead::tcp_client("example.com", 9000).tls().build();
  client->tls("/path/to/ca.pem");   // or trust a private CA
  ```

  Verification is not optional: the chain, the host name and SNI are all checked
  against the host passed to the constructor. No argument uses the system trust
  store. `connected()` only becomes true after the handshake, so a peer that
  fails verification never reports itself connected.

  No extraction refactor was needed. `ssl::stream` can borrow its next layer by
  reference, so the client keeps owning its socket and connect, socket options,
  cancel and close are untouched - only reads, writes and shutdown branch.

- Serial `rx_idle_timeout`, off by default, reopening the port after that long
  with no data received.

  ```cpp
  auto port = wirestead::serial("/dev/ttyUSB0", 115200)
                  .rx_idle_timeout(std::chrono::milliseconds(500))
                  .build();
  ```

  A device that goes quiet without erroring leaves a healthy open port and a
  read that never completes, which nothing else in the transport notices.
  Receives only, unlike the TCP idle timeout that any traffic resets - a driver
  polling a mute device would otherwise keep a bidirectional timer alive
  forever. Expiry takes the same path as a read error, so `reopen_on_error`
  decides between reopening and `Error`.

- Serial `low_latency`, on by default, asking the driver to stop batching
  received bytes behind its own timer (`ASYNC_LOW_LATENCY` on Linux).

  ```cpp
  auto port = wirestead::serial("/dev/ttyUSB0", 115200).low_latency(false).build();
  ```

  USB serial adapters ship this timer at 16 ms on FTDI parts, so a 1 ms packet
  at 115200 baud could arrive 16 ms late regardless of anything the library
  did. Best effort: drivers with no such timer refuse the request, which is
  logged at debug level and does not fail the open. See `docs/tuning.md`.
- `MessageContext::received_at()`, the steady-clock time the payload arrived.

  ```cpp
  const auto age = std::chrono::steady_clock::now() - ctx.received_at();
  ```

  Stamped at construction, which on every receive path is where the bytes come
  off the transport, so a batch handler sees one arrival time per context
  instead of the flush time, and a framer emitting several messages from one
  read gives each of them that read's time. Steady rather than system so the
  value survives a clock step; see `docs/callbacks.md` for putting it on a wall
  clock.

- Optional TLS for the TCP server, behind `-DWIRESTEAD_ENABLE_TLS=ON`. A default
  build is unchanged and has no OpenSSL dependency.

  ```cpp
  auto server = wirestead::tcp_server(9000).tls("fullchain.pem", "privkey.pem").build();
  ```

  The session layer was already talking to `TcpSocketInterface` rather than a
  socket, so this is a second implementation of that interface plus one new
  method on it - `async_handshake()`, which defaults to immediate success and so
  leaves plain sockets behaving exactly as before. Reads, writes, backpressure
  and stats are the same code either way.

  Setting only one of the certificate and key fails `start()` rather than
  falling back to plaintext.

  Closing a TLS connection sends `close_notify` without waiting for the peer's
  reply: the bidirectional exchange blocks until the peer answers, and a peer
  that simply stops reading held `stop()` for 8 seconds in testing. `on_connect`
  fires at accept, before the handshake, so a client that fails it produces a
  balanced connect/disconnect pair with no data between.

  TLS 1.2 is the floor and is not configurable. Setting `tls()` in a build
  without `WIRESTEAD_ENABLE_TLS`, or pointing it at a certificate that cannot be
  loaded, makes `start()` fail - a server asked for encryption it cannot provide
  must not come up in plaintext instead. No client certificates, peer
  verification, cipher suite selection or certificate reload; the TCP client,
  UDP, Serial and UDS remain plaintext, and DTLS is not supported. See
  `docs/security.md`.

- A one-time WARNING log the first time a channel drops queued data, pointing at
  `RuntimeStats::dropped_bytes`. `BestEffort` discards by design and
  `on_backpressure()` is not a reliable signal that it happened, so a caller who
  never polls the counters could lose data in silence. Latched per channel, not
  per drop - BestEffort can drop hundreds of thousands of times a second.

- `ServerInterface::client_stats(ClientId)` returns one connected client's
  `RuntimeStats`, or `std::nullopt` when the id has no session behind it.
  `stats()` reports the server as a whole and cannot say which client produced
  the traffic; the per-session counters existed already and were simply summed
  away at the aggregation boundary.

  Implemented for TCP and UDS. UDP returns `std::nullopt` - its virtual sessions
  are datagrams grouped by source endpoint and have no queues or counters of
  their own, so there is nothing per-client to report. A disconnected client also
  returns `std::nullopt`: its totals live on in `stats()`, but the session does
  not, so sample while it is connected if you need its numbers in isolation.

  This adds a virtual to `ServerInterface`, which changes the vtable and so
  breaks ABI. C++ ABI stability is not guaranteed before v1.0; see
  `docs/api_stability.md`. It has a default returning `std::nullopt`, so any
  out-of-tree implementer of the interface still compiles.

### Changed

- A server's cumulative `RuntimeStats` counters now survive the sessions that
  produced them. `TcpServer::stats()` and `UdsServer::stats()` aggregate live
  sessions, and a session's counters used to be destroyed along with the session
  on disconnect - so a server that had moved 795 MB reported zero the moment its
  last client went away, and anything sampling `stats()` on an interval watched
  throughput collapse on every disconnect. A closing session's totals are now
  folded into the server before it is erased.

  This applies to `bytes_*`, `messages_*`, `failed_sends`, `dropped_*` and
  `backpressure_events`; `max_queued_bytes` keeps the deepest queue any session
  reached. `queued_bytes`, `pending_bytes` and `backpressure_active` are
  unchanged - they describe live queues, and a closed session has none. The
  restart contract is unchanged too: `stop()` followed by `start()` still begins
  with zeroed counters.

### Fixed

- The TLS client no longer segfaults when a connection is torn down during a
  handshake. `close_socket()` destroyed the `ssl::stream` while an
  `asio::ssl::detail::io_op` continuation was still queued on the strand -
  `socket_.cancel()`/`close()` do not retract one - so that continuation then
  called into a freed BIO. Reproducible under parallel load and seen
  intermittently in CI as `TcpTlsLoopbackTest.ClientRejectsAnUntrustedServerCertificate`
  crashing inside `BIO_ctrl`.

  The stream now outlives the connection and is destroyed by the next
  `emplace()` on the strand, or with the transport once the io thread is
  joined. It cannot simply be moved aside instead: a pending operation holds
  the stream's original address. Whether IO is routed through TLS is tracked
  by its own flag rather than by `tls_.has_value()`.

- Every transport instance and every accepted server session eagerly allocated
  roughly 1 MiB of memory-pool buffers at construction. The six call sites
  passed `MemoryPool{50, 200}`, written while `initial_pool_size` was discarded
  and so allocated nothing; v0.9.3 made that parameter real without revisiting
  them, which turned it into 48 `new[]` calls per object. For a server the cost
  landed on the accept handler itself, before the next connection could be
  accepted - measured at 128.7 µs per session against 1.4 µs without the
  prefill - and scaled with connected clients, reaching about 1 GiB at the
  default 1024 connection limit. `enable_memory_pool(false)` did not avoid it.
  The pools now start empty, as `docs/tuning.md` already documented, and fill
  themselves as buffers are released. Callers constructing a `MemoryPool`
  directly are unaffected; prefill remains available and still defaults to 0.

  `GlobalMemoryPool::create_optimized()` and `create_size_optimized()` carried
  the same stale prefill arguments and so allocated roughly 17 MB and 26 MB up
  front since v0.9.3. Both now start empty. Their `max_pool_size` is unchanged,
  which is what those factories were for: they raise how many released buffers
  stay resident, not how many exist before the first `acquire()`.

- `TcpServer::stats()` and `UdsServer::stats()` reported `max_queued_bytes` as
  the sum of every live session's high-water mark. Peaks that occurred at
  different times were added together, so the server could report a queue depth
  that never existed: two sessions that each peaked at 200 KiB an hour apart
  were reported as 400 KiB. The server-wide value is now the largest depth any
  one session reached. Servers with a single connected client are unaffected.
  The remaining fields are genuine totals and keep summing.

### Compatibility

- Additive at the symbol level: the shared library exports 1225 symbols against
  v0.9.3's 1191, with **none removed**. The 34 new ones are the TLS builder and
  wrapper entry points, `multicast_group()`, the serial `low_latency`/`rs485`/
  `dtr`/`rts`/`rx_idle_timeout` setters, `client_stats()`, `LengthPrefixFramer`
  and its vtable, and `concurrency::set_io_thread_init()`. Measured with
  `nm -D --defined-only` on Release builds of the tag and of v0.9.3.

- **Read this before upgrading: a patch release that does change two default
  behaviors.** `docs/api_stability.md` says patch releases *should* avoid
  changing existing default behavior, and this one does it twice, so the
  exceptions are named here rather than left to be discovered.

  A server's cumulative counters now survive the sessions that produced them,
  so `TcpServer::stats()` and `UdsServer::stats()` no longer collapse to zero
  when the last client disconnects, and `max_queued_bytes` is the deepest queue
  one session reached rather than the sum of every session's peak. Code that
  samples `stats()` on an interval sees different numbers without changing a
  line. Both were previously wrong rather than merely different - a server that
  had moved 795 MB reported zero the moment its last client went away - which
  is why the fix landed in the 0.9 line instead of waiting.

  The other is serial `low_latency`, below.

- TLS is opt-in and off by default. `WIRESTEAD_ENABLE_TLS=ON` adds OpenSSL as a
  link dependency; a build that does not ask for it gains no new dependency,
  and the TLS entry points compile to nothing.

- Serial `low_latency` defaults to **on**, which is a behavior change for USB
  serial adapters: the driver's receive latency timer is cleared where the
  platform supports it. Ports that refuse the ioctl are unaffected and the
  refusal is not an error. `rs485`, `dtr` and `rts` are engaged only when set.

- Consumers must rebuild, as for any pre-1.0 release; see
  `docs/api_stability.md`.

## v0.9.3 - 2026-08-08

### Breaking

- Builder setters all return `Derived&` and mutate in place. `on_data`,
  `on_data_batch`, `on_message`, `on_message_batch` and `on_error` used to
  return a *new* builder of a different type and leave the original
  moved-from, while `on_connect`, `framer`, `auto_start` and the rest returned
  a reference — the same builder, two conventions. The `BuilderState` template
  parameter, the `Rebind` alias and the CRTP machinery behind them are gone.

  **This bought nothing.** Nothing ever read `BuilderState`; `ibuilder.hpp`
  said so itself, documenting it as "not for mandatory build gating". It only
  created a trap — take the result of `on_data` and keep using the original and
  you are working with a moved-from object, which `[[nodiscard]]` cannot catch
  — and forced 28 explicit template instantiations of otherwise identical code.

  Chained code is unaffected, which is how the quickstart and every example are
  written:

  ```cpp
  auto client = wirestead::tcp_client("127.0.0.1", 8080)
                    .on_data(...)
                    .on_error(...)
                    .build();          // unchanged
  ```

  Two forms need updating. Spelling the type with its state:

  ```cpp
  builder::TcpClientBuilder<> b{...};                    // before
  builder::TcpClientBuilder b{...};                      // after
  ```

  And capturing the result of a handler setter, which used to hand back a new
  object:

  ```cpp
  auto b = builder::UdpClientBuilder(0).on_data_batch(h); // before
  auto udp = std::move(b).auto_start(false).build();

  auto b = builder::UdpClientBuilder(0);                  // after
  b.on_data_batch(h);
  auto udp = b.auto_start(false).build();
  ```

  `TcpClientBuilderDefault` and the other `*Default` aliases still name the
  builder, so code using those keeps compiling.


### Added

- `read_buffer_size` on the TCP and UDS configs, wrappers, and builders. This
  is the userspace buffer each read fills, and it was previously a
  compile-time-fixed 4 KiB `std::array` per connection with no way to change
  it — `receive_buffer_size` only ever set the kernel's `SO_RCVBUF`. Raising it
  cuts read completions and callback dispatches on bulk transfers. The default
  is unchanged at 4 KiB, and values are clamped to
  `[MIN_READ_BUFFER_SIZE, MAX_READ_BUFFER_SIZE]` (512 B to 1 MiB). The ceiling
  is far below `MAX_SOCKET_BUFFER_SIZE` because this buffer is per connection
  and a server multiplies it by `max_connections`.
- `read_chunk` on the serial wrapper and builder. Serial already had the knob
  on its config and the transport already honoured it, but with no wrapper or
  builder surface it was reachable only by constructing a `SerialConfig` by
  hand — serial was the one transport whose read buffer could not be set the
  way TCP and UDS set `read_buffer_size`. It is the same setting under the
  older name, and is now clamped to the same
  `[MIN_READ_BUFFER_SIZE, MAX_READ_BUFFER_SIZE]` bounds, which also closes a
  hole where `read_chunk = 0` reached the transport as `rx_.resize(0)`.

### Changed

- `MemoryPool`'s `initial_pool_size` now prefills the buckets instead of being
  discarded, and its default moves from 400 to 0. **If you pass this argument
  explicitly, the pool now really does pre-allocate**; previously the value was
  ignored and every pool started empty. The default changes to 0 so that
  behaviour is preserved for callers who never set it: the buckets run 1 KiB to
  64 KiB, so honouring the old nominal 400 would have reserved roughly 8.3 MiB
  before the first `acquire()` — a cost the old signature implied and never
  charged, and one that matters on the embedded targets this library targets.
- Stream transports now drain several queued buffers into one scatter-gather
  write instead of one send syscall per queued message. Measured on a TCP
  loopback burst of 16386 small messages, send syscalls drop from 16386 to
  1025. The socket interfaces gained a buffer-sequence `async_write` overload
  with a correct default implementation, so existing implementations keep
  working unchanged.
- Removed the per-chunk heap allocation and copy from the receive path.
  `MessageContext` now borrows the payload for the single-shot `on_data()` and
  `on_message()` dispatch instead of copying it into an owned buffer; only the
  batch handlers, whose contexts are queued until the batch is flushed, still
  own their data. Measured on a TCP loopback echo, allocations on the receiving
  io thread drop from 3 to 2 per callback. The documented callback contract is
  unchanged - `docs/callbacks.md` already scoped the view to the callback - and
  copying a `MessageContext` still takes ownership, so keeping one past the
  callback remains safe.
- `MessageContext::safe_data()` materializes its buffer on demand when the
  context borrows its payload. It is now the only accessor that copies; prefer
  `data()`, `data_as_string()`, or `data_as_vector()`.
- Transports and wrappers now snapshot their callbacks through a shared pointer
  instead of copying a `std::function` on every dispatch. A `std::function`
  copy heap-allocates whenever the target outgrows its small-object buffer, and
  the receive path took one such copy per chunk at each layer. Storage
  discipline is unchanged - the same mutex guards the member and the callback
  is still invoked outside the lock. Together with the change above, the TCP
  client receive path goes from 5.06 to 0.06 allocations per callback, measured
  on a loopback echo with a handler capturing 64 bytes; receiving is now
  allocation-free.

### Fixed

- `UdpChannel::on_bytes_from()` now takes `callback_mtx_` when installing the
  callback. It assigned without the lock while the strand-confined read site
  took it, against the member's own documented invariant, so replacing the
  callback on a running channel raced with the receive path.

### Compatibility

- The shared library exports 440 fewer symbols than v0.9.2: 1366 down to 1057.
  436 of those are builder template instantiations that no longer exist —
  collapsing `Rebind` removed four instantiations per builder across seven
  builders. The remaining four are `TcpServerSession` and `UdsServerSession`
  constructors whose signatures changed with the gather-write work. No
  Wirestead API was removed other than the builder state machinery described
  under Breaking.
- Consumers must rebuild, as for any pre-1.0 release; see
  `docs/api_stability.md`.

## v0.9.2 - 2026-08-01

### Added

- Documented the Unilink to Wirestead migration path and compatibility policy.

### Changed

- Clarified that the official vcpkg port is the canonical package-consumer
  install path.
- Aligned the CMake project description with the GitHub repository
  description. This string is user-visible: it is carried into the CPack
  package metadata and the generated pkg-config description.
- Drive link-time optimization through CMake's `INTERPROCEDURAL_OPTIMIZATION`
  property instead of hand-written `/GL`, `/LTCG`, and `-flto` flags, applied
  per library target and only to the optimized configurations.
  `WIRESTEAD_ENABLE_LTO` now means the same thing on every compiler; it
  previously gated `-flto` on GCC and Clang while MSVC received `/GL`
  unconditionally in Release, so the option had no effect there.
- Removed the MSVC workaround that forced `/GL-` and `/LTCG:OFF` onto the
  library targets to avoid `link.exe` access violations. A CI run with the
  override removed confirmed the underlying problem no longer reproduces, and
  the flags it fought with are gone.
- Documented that out-of-line functions declared in internal headers are not
  always exported from the shared library, so code calling them links against
  the static library but not the shared one. The documented public surface is
  exported from both.
- Extended the install-and-consume CI job to macOS and Windows. It previously
  ran on Linux only, so the installed-package path was never exercised on any
  other platform.
- Added a CI check that inspects the installed shared library and fails if it
  exports no Wirestead symbols or any third-party ones. The unit tests link the
  static library, so nothing else looks at what the shared library exports.
- Pinned every `microsoft/vcpkg` checkout to a reviewed commit recorded in
  `VCPKG_BASELINE`, instead of cloning whatever is on its default branch at
  build time. This covers CI, the CMake matrix, the release workflow, the
  vcpkg package test, the install-and-consume job, and the `setup-vcpkg`
  composite action. The Windows x64 legs previously used the vcpkg that ships
  in the runner image, which meant release binaries depended on whichever
  version that image happened to carry.
- Added a weekly `vcpkg baseline bump` workflow that proposes a
  `VCPKG_BASELINE` update as a reviewable pull request, so the pin can be kept
  current without relying on someone remembering to move it.

### Deprecated

- Unilink compatibility names remain deprecated compatibility surfaces for the
  v0.9.x line.

### Fixed

- Made the TCP client retry tests wait for the attempts they assert on instead
  of running for a fixed duration and hoping. They raced anything that slowed
  the machine down, and one of them failed a coverage run on a resolve that
  took longer than the window allowed.
- Stopped the shared library from re-exporting `boost::asio::detail` symbols it
  merely links against. Those are weak and unique symbols that hidden
  visibility does not remove, so a consumer loading a different Boost into the
  same process could have them merged, which is an ODR violation. No
  `wirestead` symbol changed: the export surface this project owns, including
  the typeinfo and vtables needed to catch exceptions across the library
  boundary, is unchanged.
- Fixed the Windows consumer sample linking against a different MSVC runtime
  than the static vcpkg triplet its dependencies were built with, which
  surfaced as LNK2038 and LNK2005 errors after upstream vcpkg drift.

### Compatibility

- The shared library no longer exports symbols this project does not own.
  Every `wirestead` symbol is still exported, including those from internal
  namespaces and the typeinfo and vtables that exceptions thrown across the
  library boundary need, so no Wirestead API was removed. Code that resolved a
  `boost`, `spdlog`, or `fmt` symbol out of `libwirestead` rather than linking
  it directly must now link it directly; that was never a supported use.
- No intentional public API removal was made in v0.9.2.
- Known limitations remain the pre-1.0 ABI policy and the v0.9.x Unilink
  compatibility layer described in `docs/migration-from-unilink.md`.

## v0.9.1 - 2026-07-26

### Changed

- Linked the published `wirestead-docs` site from the README and updated
  satellite repository links to the `wirestead` organization.
- Updated vcpkg references after the `wirestead` port became available from the
  vcpkg registry.
- Moved docs and coverage Pages ownership to `wirestead-docs`.
- Updated GitHub Actions dependency pins, including `actions/setup-python`.

### Fixed

- Addressed correctness and concurrency findings in builders, configuration
  management, framers, callback guards, TCP/UDP/UDS wrappers, serial wrapper,
  and related tests.
- Fixed Doxygen checkout path handling so generated API reference content is
  produced in the docs workflow.
- Fixed dependabot auto-merge strategy.

### Compatibility

- No intentional public API removal was made in v0.9.1.
- Known limitations remain the pre-1.0 ABI policy and the v0.9.x Unilink
  compatibility layer described in `docs/migration-from-unilink.md`.

## v0.9.0 - 2026-07-19

### Changed

- Renamed the canonical project identity from Unilink to Wirestead.
- Changed the canonical C++ namespace from `unilink` to `wirestead`.
- Changed canonical include paths from `<unilink/...>` to `<wirestead/...>`.
- Changed the canonical CMake package from `unilink` to `wirestead`.
- Changed the canonical CMake target from `unilink::unilink` to
  `wirestead::wirestead`.
- Changed generated library names, release archive names, CPack metadata, and
  pkg-config metadata from Unilink names to Wirestead names.
- Changed canonical CMake options, compile definitions, export macros, and
  runtime environment variables from `UNILINK_*` names to `WIRESTEAD_*` names.
- Changed package identity to the `wirestead` vcpkg port. The former
  `jwsung91-unilink` port is a deprecated compatibility alias.

### Compatibility

- v0.9.0 is an ABI break from v0.8.x. Consumers must rebuild binaries and
  libraries that link to the C++ core.
- `namespace unilink = wirestead` is provided for source compatibility.
- `<unilink/...>` forwarding headers are installed for the v0.9.x line.
- `find_package(unilink CONFIG REQUIRED)` and `unilink::unilink` remain as
  compatibility surfaces that forward to the Wirestead package and target.
- `UNILINK_*` CMake inputs are accepted as compatibility aliases for matching
  `WIRESTEAD_*` options. Conflicting old/new values fail at configure time.
- `UNILINK_API`, `UNILINK_EXPORT`, `UNILINK_NO_EXPORT`, and Unilink logging
  macros are compatibility aliases for the Wirestead macros.
- `UNILINK_LOG_LEVEL` is read only when `WIRESTEAD_LOG_LEVEL` is unset or empty.
- A `libunilink` filename or SONAME compatibility shim is not provided because
  old v0.8.x binaries are not ABI-compatible with v0.9.x symbols.

### Fixed

- Stabilized UDP client restart lifecycle coverage before the v0.9.0 final tag.
- Made pkg-config files relocatable.
- Fixed release workflow permissions and retry behavior during the v0.9.0
  release candidate cycle.

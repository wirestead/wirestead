/*
 * Copyright 2025 Jinwoo Sung
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Reproduction for the v0.10 contract audit, rule C-5.4-3 (serial):
// Serial::stop() joins the transport's own io thread without checking whether
// it is the current thread, so stop() called from inside a serial callback
// joins that thread with itself.
//
// This program REPRODUCES and REPORTS. It asserts nothing about what stop()
// should do, so it does not freeze today's behavior into a test: it fails only
// if the repro path cannot run, or if a scenario hangs.
//
// Two things are deliberately kept apart:
//
//   shutdown scenarios (stop-nocatch, stop-catch, control)
//       observe the callback, the stop() call and an outside stop(), then
//       destroy the object with no call in flight;
//   restart scenarios (restart-after-callback-stop, restart-control)
//       observe only whether a restart returns. If it does not return within
//       the bound, the child reports and _exit()s **without destroying the
//       object**, because destroying it while start() is in flight would make
//       the observation unsafe rather than informative.
//
// Every scenario runs in a forked child with a parent-side timeout, so a hang
// kills the child instead of the test runner.
//
// Exit codes: 0 scenario completed (or skipped), 2 setup failed, 3 the child
// hung, 4 the child stopped deliberately with a call still in flight.

#include <cstdio>
#include <cstring>

#if defined(_WIN32)
int main() {
  std::printf("[skip] POSIX pseudo-terminal required\n");
  return 0;
}
#else

#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "wirestead/wirestead.hpp"

using namespace std::chrono_literals;

namespace {

void obs(const char* line) {
  std::printf("[obs] %s\n", line);
  std::fflush(stdout);
}

// Set from the callback's own scope exit, so it fires whether the callback
// returns normally or leaves through an exception. It reports that the user
// callback body was left - not that the library finished its own handling.
struct ScopeSignal {
  std::atomic<bool>& flag;
  ~ScopeSignal() { flag.store(true); }
};

struct Pty {
  int master = -1;
  std::string slave;
};

bool open_pty(Pty& pty) {
  pty.master = posix_openpt(O_RDWR | O_NOCTTY);
  if (pty.master < 0 || grantpt(pty.master) != 0 || unlockpt(pty.master) != 0) return false;
  const char* name = ptsname(pty.master);
  if (name == nullptr) return false;
  pty.slave = name;
  return true;
}

// Reports one of: returned-true, returned-false, threw, or still-running.
void observe_start(wirestead::wrapper::Serial& port, const char* label) {
  try {
    auto future = port.start();
    const auto status = future.wait_for(3s);
    if (status != std::future_status::ready) {
      std::printf("[obs] %s: has not returned after 3s\n", label);
      std::fflush(stdout);
      obs("leaving the object alive: a start() is still in flight");
      std::fflush(stdout);
      _exit(4);
    }
    const bool ok = future.get();
    std::printf("[obs] %s: returned %s\n", label, ok ? "true" : "false");
    std::fflush(stdout);
  } catch (const std::exception& e) {
    std::printf("[obs] %s: threw %s\n", label, e.what());
    std::fflush(stdout);
  } catch (...) {
    std::printf("[obs] %s: threw an unknown exception\n", label);
    std::fflush(stdout);
  }
}

enum class Mode { StopNoCatch, StopCatch, Control, RestartAfterCallbackStop, RestartControl };

bool calls_stop_in_callback(Mode m) {
  return m == Mode::StopNoCatch || m == Mode::StopCatch || m == Mode::RestartAfterCallbackStop;
}
bool catches_at_call_site(Mode m) { return m == Mode::StopCatch; }
bool observes_restart(Mode m) { return m == Mode::RestartAfterCallbackStop || m == Mode::RestartControl; }

int run_scenario(Mode mode) {
  Pty pty;
  if (!open_pty(pty)) {
    obs("pty-setup-failed");
    return 2;
  }
  std::printf("[obs] pty=%s\n", pty.slave.c_str());
  std::fflush(stdout);

  // Default construction: the transport owns its io_context and io thread,
  // which is the configuration the audit row is about.
  auto port = wirestead::serial(pty.slave, 115200).on_error([](auto&&) {}).build();

  std::atomic<bool> entered{false};
  std::atomic<bool> callback_left{false};
  std::atomic<bool> stop_returned{false};
  std::atomic<bool> threw_at_call_site{false};

  port->on_data([&](const wirestead::wrapper::MessageContext&) {
    if (entered.exchange(true)) return;
    ScopeSignal leave{callback_left};
    obs("callback-entered");
    if (!calls_stop_in_callback(mode)) {
      obs("control: callback does not call stop()");
      return;
    }
    if (catches_at_call_site(mode)) {
      try {
        port->stop();
        stop_returned = true;
        obs("stop-returned-normally");
      } catch (const std::exception& e) {
        threw_at_call_site = true;
        std::printf("[obs] stop-threw-at-call-site: %s\n", e.what());
        std::fflush(stdout);
      } catch (...) {
        threw_at_call_site = true;
        obs("stop-threw-at-call-site: unknown exception");
      }
    } else {
      // No catch here: whatever escapes goes to the library's own callback
      // dispatch, which is one of the observation points.
      port->stop();
      stop_returned = true;
      obs("stop-returned-normally");
    }
  });

  if (!port->start_sync()) {
    obs("start-failed");
    return 2;
  }
  obs("started");

  const char payload[] = "ping\n";
  if (write(pty.master, payload, sizeof(payload) - 1) < 0) {
    obs("write-to-pty-failed");
    return 2;
  }

  for (int i = 0; i < 250 && !entered.load(); ++i) std::this_thread::sleep_for(20ms);
  if (!entered.load()) {
    obs("callback-never-entered");
    return 2;
  }
  // Wait for the callback body to be left, by return or by exception. Without
  // this the flags below would be read while the callback may still be running.
  for (int i = 0; i < 250 && !callback_left.load(); ++i) std::this_thread::sleep_for(20ms);
  if (!callback_left.load()) {
    obs("callback-did-not-return-within-5s");
    obs("leaving the object alive: a callback is still in flight");
    return 4;
  }

  std::printf("[obs] callback-left: stop_returned=%d threw_at_call_site=%d connected=%d\n",
              static_cast<int>(stop_returned.load()), static_cast<int>(threw_at_call_site.load()),
              static_cast<int>(port->connected()));
  std::fflush(stdout);

  obs("outside-stop-begin");
  port->stop();
  obs("outside-stop-returned");

  if (observes_restart(mode)) {
    // Only this scenario touches start() again. It never destroys the object
    // afterwards unless start() has actually returned.
    observe_start(*port, "restart-after-outside-stop");
    obs("restart returned, so stopping again before destruction");
    port->stop();
  }

  obs("destroying");
  port.reset();
  obs("destroyed");
  close(pty.master);
  return 0;
}

Mode parse_mode(const char* text) {
  if (std::strcmp(text, "stop-catch") == 0) return Mode::StopCatch;
  if (std::strcmp(text, "control") == 0) return Mode::Control;
  if (std::strcmp(text, "restart-after-callback-stop") == 0) return Mode::RestartAfterCallbackStop;
  if (std::strcmp(text, "restart-control") == 0) return Mode::RestartControl;
  return Mode::StopNoCatch;
}

}  // namespace

int main(int argc, char** argv) {
  const char* mode_text = argc > 1 ? argv[1] : "stop-nocatch";
  const int timeout_s = argc > 2 ? atoi(argv[2]) : 20;
  std::printf("[obs] mode=%s timeout=%ds\n", mode_text, timeout_s);
  std::fflush(stdout);

  pid_t pid = fork();
  if (pid < 0) {
    obs("fork-failed");
    return 2;
  }
  if (pid == 0) _exit(run_scenario(parse_mode(mode_text)));

  for (int i = 0; i < timeout_s * 20; ++i) {
    int status = 0;
    if (waitpid(pid, &status, WNOHANG) == pid) {
      if (WIFEXITED(status)) {
        const int code = WEXITSTATUS(status);
        std::printf("[result] child exited code=%d\n", code);
        std::fflush(stdout);
        // 4 means the child stopped on purpose with a call in flight: that is
        // an observation, not a failure of the reproduction.
        return (code == 0 || code == 4) ? 0 : 2;
      }
      std::printf("[result] child killed by signal %d\n", WIFSIGNALED(status) ? WTERMSIG(status) : 0);
      std::fflush(stdout);
      return 2;
    }
    std::this_thread::sleep_for(50ms);
  }

  kill(pid, SIGKILL);
  waitpid(pid, nullptr, 0);
  std::printf("[result] child hung and was killed after %ds\n", timeout_s);
  std::fflush(stdout);
  return 3;
}

#endif

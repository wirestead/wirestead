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
// if the repro path cannot run, or if the scenario hangs.
//
// The scenario runs in a forked child with a parent-side timeout, so a hang
// kills the child instead of the test runner.
//
// Modes:
//   nocatch  - call stop() in the callback, let the exception propagate
//   catch    - call stop() in the callback inside try/catch at the call site
//   control  - do not call stop() in the callback (baseline for the follow-on)
//
// Exit codes: 0 repro completed (or skipped), 2 setup failed, 3 the child hung.

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
#include <string>
#include <thread>

#include "wirestead/wirestead.hpp"

using namespace std::chrono_literals;

namespace {

void obs(const char* line) {
  std::printf("[obs] %s\n", line);
  std::fflush(stdout);
}

int run_scenario(bool catch_at_call_site, bool control) {
  int master = posix_openpt(O_RDWR | O_NOCTTY);
  if (master < 0 || grantpt(master) != 0 || unlockpt(master) != 0) {
    obs("pty-setup-failed");
    return 2;
  }
  const char* slave = ptsname(master);
  if (slave == nullptr) {
    obs("ptsname-failed");
    return 2;
  }
  std::printf("[obs] pty=%s\n", slave);
  std::fflush(stdout);

  // Default construction: the transport owns its io_context and io thread,
  // which is the configuration the audit row is about.
  auto port = wirestead::serial(slave, 115200).on_error([](auto&&) {}).build();

  std::atomic<bool> entered{false};
  std::atomic<bool> stop_returned{false};
  std::atomic<bool> threw{false};

  port->on_data([&](const wirestead::wrapper::MessageContext&) {
    if (entered.exchange(true)) return;
    obs("callback-entered");
    if (control) {
      obs("control: callback does not call stop()");
      stop_returned = true;
      return;
    }
    if (catch_at_call_site) {
      try {
        port->stop();
        stop_returned = true;
        obs("stop-returned-normally");
      } catch (const std::exception& e) {
        threw = true;
        std::printf("[obs] stop-threw-at-call-site: %s\n", e.what());
        std::fflush(stdout);
      } catch (...) {
        threw = true;
        obs("stop-threw-at-call-site: unknown exception");
      }
    } else {
      // No catch here: whatever escapes is handled by the library's own
      // callback dispatch, which is one of the observation points.
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
  if (write(master, payload, sizeof(payload) - 1) < 0) {
    obs("write-to-pty-failed");
    return 2;
  }

  for (int i = 0; i < 100 && !entered.load(); ++i) std::this_thread::sleep_for(20ms);
  if (!entered.load()) {
    obs("callback-never-entered");
    return 2;
  }
  for (int i = 0; i < 100 && !(stop_returned.load() || threw.load()); ++i) std::this_thread::sleep_for(20ms);

  std::printf("[obs] after-callback: stop_returned=%d threw_at_call_site=%d connected=%d\n",
              static_cast<int>(stop_returned.load()), static_cast<int>(threw.load()),
              static_cast<int>(port->connected()));
  std::fflush(stdout);

  // Follow-on: how much of the shutdown actually ran. A stop() from outside,
  // then a restart, each bounded so this cannot hang on its own.
  obs("outside-stop-begin");
  port->stop();
  obs("outside-stop-returned");

  std::atomic<bool> restarted{false};
  std::thread restarter([&] { restarted = port->start_sync(); });
  for (int i = 0; i < 60 && !restarted.load(); ++i) std::this_thread::sleep_for(50ms);
  std::printf("[obs] restart-after-stop: %s\n", restarted.load() ? "succeeded" : "did not complete within 3s");
  std::fflush(stdout);
  if (restarted.load()) {
    restarter.join();
    port->stop();
  } else {
    restarter.detach();
  }

  obs("destroying");
  port.reset();
  obs("destroyed");
  close(master);
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  const char* mode = argc > 1 ? argv[1] : "nocatch";
  const int timeout_s = argc > 2 ? atoi(argv[2]) : 20;
  const bool catch_at_call_site = std::strcmp(mode, "catch") == 0;
  const bool control = std::strcmp(mode, "control") == 0;
  std::printf("[obs] mode=%s timeout=%ds\n", mode, timeout_s);
  std::fflush(stdout);

  pid_t pid = fork();
  if (pid < 0) {
    obs("fork-failed");
    return 2;
  }
  if (pid == 0) _exit(run_scenario(catch_at_call_site, control));

  for (int i = 0; i < timeout_s * 20; ++i) {
    int status = 0;
    if (waitpid(pid, &status, WNOHANG) == pid) {
      if (WIFEXITED(status)) {
        const int code = WEXITSTATUS(status);
        std::printf("[result] child exited code=%d\n", code);
        std::fflush(stdout);
        return code == 0 ? 0 : 2;
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

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

#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <thread>

#include "wirestead/concurrency/io_context_manager.hpp"

using namespace wirestead::concurrency;

namespace wirestead {
namespace test {

TEST(IoContextManagerLifecycleTest, ExternalContextReference) {
  boost::asio::io_context external_ioc;
  {
    IoContextManager manager(external_ioc);
    EXPECT_EQ(&manager.get_context(), &external_ioc);
    manager.start();  // Should return early
    manager.stop();   // Should return early
  }
}

TEST(IoContextManagerLifecycleTest, StopFromWithinThread) {
  IoContextManager manager;
  manager.start();

  bool stop_attempted = false;
  boost::asio::post(manager.get_context(), [&]() {
    manager.stop();  // Should log error
    stop_attempted = true;
  });

  // Wait for it
  auto start = std::chrono::steady_clock::now();
  while (!stop_attempted && std::chrono::steady_clock::now() - start < std::chrono::seconds(1)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_TRUE(stop_attempted);
  manager.stop();  // Proper stop from outside
}

TEST(IoContextManagerLifecycleTest, StartFromWithinThread) {
  IoContextManager manager;
  manager.start();

  bool start_attempted = false;
  boost::asio::post(manager.get_context(), [&]() {
    manager.start();  // Should return early and log error
    start_attempted = true;
  });

  auto start = std::chrono::steady_clock::now();
  while (!start_attempted && std::chrono::steady_clock::now() - start < std::chrono::seconds(1)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_TRUE(start_attempted);
  manager.stop();
}

TEST(IoContextManagerLifecycleTest, RestartStoppedContext) {
  IoContextManager manager;
  manager.get_context().stop();
  EXPECT_TRUE(manager.get_context().stopped());

  manager.start();  // Should call restart()
  EXPECT_TRUE(manager.is_running());
  manager.stop();
}

TEST(IoContextManagerLifecycleTest, IndependentContext) {
  auto ioc = IoContextManager::instance().create_independent_context();
  EXPECT_NE(ioc, nullptr);
}

}  // namespace test
}  // namespace wirestead

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

#pragma once
#include <boost/asio.hpp>
#include <memory>
#include <thread>

namespace wirestead::test {
// Tests normally driving an external context in run_for() slices must
// continue its progress while an outside stop waits for actual cleanup.
template <typename Channel>
void stop_with_context(const std::shared_ptr<Channel>& channel, boost::asio::io_context& io) {
  if (io.stopped()) io.restart();
  auto work = boost::asio::make_work_guard(io);
  std::jthread runner([&] { io.run(); });
  channel->stop();
  work.reset();
  io.stop();
  runner.join();
  io.restart();
}
template <typename Wrapper>
void stop_wrapper_with_context(Wrapper& wrapper, boost::asio::io_context& io) {
  if (io.stopped()) io.restart();
  auto work = boost::asio::make_work_guard(io);
  std::jthread runner([&] { io.run(); });
  wrapper.stop();
  work.reset();
  io.stop();
  runner.join();
  io.restart();
}
}  // namespace wirestead::test

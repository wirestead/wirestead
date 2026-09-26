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

#include <boost/asio/steady_timer.hpp>
#include <vector>

#include "wirestead/wrapper/bounded_receive.hpp"

namespace wirestead::wrapper::detail {
// Access is protected by the owning wrapper mutex. Timer handlers run on the
// session executor; weak ownership and run gates exclude retired sessions.
struct SessionBatch {
  SessionBatch(boost::asio::any_io_executor executor, std::shared_ptr<ReceiveScope> scope)
      : receive(std::move(scope)), timer(std::move(executor)) {}
  ReceiveState receive;
  ReceiveBatch data;
  ReceiveBatch messages;
  boost::asio::steady_timer timer;
  bool scheduled = false;
  ~SessionBatch() { timer.cancel(); }
};
}  // namespace wirestead::wrapper::detail

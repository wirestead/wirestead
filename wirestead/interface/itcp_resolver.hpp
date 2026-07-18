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
#include <functional>

#include "wirestead/base/platform.hpp"
#include "wirestead/base/visibility.hpp"

namespace wirestead {
namespace interface {

namespace net = boost::asio;

/**
 * @brief An interface abstracting Boost.Asio's tcp::resolver for testability.
 * This is an internal interface used for dependency injection and mocking.
 */
class WIRESTEAD_API TcpResolverInterface {
 public:
  virtual ~TcpResolverInterface() = default;

  virtual void async_resolve(
      const std::string& host, const std::string& service,
      std::function<void(const boost::system::error_code&, net::ip::tcp::resolver::results_type)> handler) = 0;
};

}  // namespace interface
}  // namespace wirestead

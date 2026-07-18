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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "wirestead/base/visibility.hpp"
#include "wirestead/config/serial_config.hpp"
#include "wirestead/diagnostics/error_types.hpp"
#include "wirestead/interface/channel.hpp"

namespace boost {
namespace asio {
class io_context;
}
}  // namespace boost

namespace wirestead {

namespace interface {
class SerialPortInterface;
}

namespace transport {

/**
 * @brief Serial Transport implementation
 */
class WIRESTEAD_API Serial : public interface::Channel, public std::enable_shared_from_this<Serial> {
 public:
  // use_shared_context: opt into the shared IoContextManager singleton
  // instead of the default dedicated io_context + thread. Only meaningful
  // for multi-instance-per-process deployments deliberately trading
  // parallelism for reduced thread/memory overhead (#440); most callers
  // should leave this false.
  static std::shared_ptr<Serial> create(const config::SerialConfig& cfg, bool use_shared_context = false);
  static std::shared_ptr<Serial> create(const config::SerialConfig& cfg, boost::asio::io_context& ioc);
  static std::shared_ptr<Serial> create(const config::SerialConfig& cfg,
                                        std::unique_ptr<interface::SerialPortInterface> port,
                                        boost::asio::io_context& ioc);
  ~Serial() override;

  // Move semantics
  Serial(Serial&&) noexcept;
  Serial& operator=(Serial&&) noexcept;

  // Non-copyable
  Serial(const Serial&) = delete;
  Serial& operator=(const Serial&) = delete;

  // Channel implementation
  void start() override;
  void stop() override;
  bool is_connected() const override;
  bool is_backpressure_active() const override;
  wrapper::RuntimeStats stats() const override;
  void reset_stats() override;
  std::optional<diagnostics::ErrorInfo> last_error_info() const override;
  boost::asio::any_io_executor get_executor() override;

  bool async_write_copy(memory::ConstByteSpan data) override;
  bool async_write_move(std::vector<uint8_t>&& data) override;
  bool async_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) override;
  bool async_try_write_copy(memory::ConstByteSpan data) override;
  bool async_try_write_move(std::vector<uint8_t>&& data) override;
  bool async_try_write_shared(std::shared_ptr<const std::vector<uint8_t>> data) override;

  void on_bytes(OnBytes cb) override;
  void on_state(OnState cb) override;
  void on_backpressure(OnBackpressure cb) override;

  void set_backpressure_strategy(base::constants::BackpressureStrategy strategy);
  void set_retry_interval(unsigned interval_ms);

 private:
  explicit Serial(const config::SerialConfig& cfg, bool use_shared_context);
  Serial(const config::SerialConfig& cfg, std::unique_ptr<interface::SerialPortInterface> port,
         boost::asio::io_context& ioc);

  struct Impl;
  const Impl* get_impl() const { return impl_.get(); }
  Impl* get_impl() { return impl_.get(); }
  std::unique_ptr<Impl> impl_;
};
}  // namespace transport
}  // namespace wirestead

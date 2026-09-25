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

#include <mutex>

#include "wirestead/interface/connection_channel.hpp"

namespace wirestead::test {
// Shared connection bookkeeping for controlled wrapper fixtures. Each fixture
// supplies typed admission decisions and calls connection_opened/lost at its
// simulated lifecycle transitions. Wrapper sends retain the fixture's owner.
class TestConnectionChannel : public interface::ConnectionChannel {
 public:
  using SendRejection = wrapper::SendRejection;
  struct Record {
    std::optional<SendRejection> ended;
    void end(SendRejection reason) {
      if (!ended) ended = reason;
    }
  };
  class Pin : public interface::WriteConnection {
   public:
    Pin(TestConnectionChannel& owner, std::shared_ptr<Record> record) : owner_(owner), record_(std::move(record)) {}
    std::optional<SendResult> poll_capacity() override {
      std::lock_guard<std::mutex> lock(owner_.connection_mutex_);
      if (record_->ended) return SendResult::reject(*record_->ended);
      if (!owner_.is_connected()) return SendResult::reject(SendRejection::NotReady);
      if (owner_.is_backpressure_active()) return std::nullopt;
      return SendResult::accept();
    }
    SendResult write_copy(memory::ConstByteSpan data) override {
      return admit([&] { return owner_.async_write_copy_result(data); });
    }
    SendResult write_move(std::vector<uint8_t>&& data) override {
      return admit([&] { return owner_.async_write_move_result(std::move(data)); });
    }
    SendResult write_shared(std::shared_ptr<const std::vector<uint8_t>> data) override {
      return admit([&] { return owner_.async_write_shared_result(std::move(data)); });
    }

   private:
    template <typename Write>
    SendResult admit(Write write) {
      std::lock_guard<std::mutex> lock(owner_.connection_mutex_);
      if (record_->ended || owner_.connection_ != record_ || !owner_.is_connected())
        return SendResult::reject(SendRejection::NotReady);
      return write();
    }
    TestConnectionChannel& owner_;
    std::shared_ptr<Record> record_;
  };
  CaptureResult capture_write_connection() override {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    if (!is_connected() || connection_->ended) return SendRejection::NotReady;
    return std::make_shared<Pin>(*this, connection_);
  }
  void cancel_write_waits() override {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    connection_->end(SendRejection::CancelledWhileWaiting);
  }

 protected:
  void connection_opened() {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    connection_->end(SendRejection::NotReady);
    connection_ = std::make_shared<Record>();
  }
  void connection_lost() {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    connection_->end(SendRejection::NotReady);
  }

 private:
  std::mutex connection_mutex_;
  std::shared_ptr<Record> connection_ = std::make_shared<Record>();
};
}  // namespace wirestead::test

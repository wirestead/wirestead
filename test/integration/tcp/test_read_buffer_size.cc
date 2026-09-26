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

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>

#include "test_utils.hpp"
#include "wirestead/wirestead.hpp"

using namespace wirestead;
using namespace std::chrono_literals;

namespace {

constexpr size_t kPayloadBytes = 512 * 1024;
constexpr size_t kLargeReadBuffer = 64 * 1024;

struct ReceiveTally {
  std::mutex mtx;
  size_t total = 0;
  size_t largest_chunk = 0;

  void record(size_t n) {
    std::lock_guard<std::mutex> lock(mtx);
    total += n;
    if (n > largest_chunk) largest_chunk = n;
  }
};

struct TransferResult {
  size_t total = 0;
  size_t largest_chunk = 0;
};

// Drives kPayloadBytes from a client into a server configured with
// `read_buffer_size`, and reports what the server saw.
TransferResult run_transfer(uint16_t port, size_t read_buffer_size) {
  auto tally = std::make_shared<ReceiveTally>();

  auto server = wirestead::tcp_server(port).read_buffer_size(read_buffer_size).build();
  server->on_data([tally](const wirestead::MessageContext& ctx) { tally->record(ctx.data().size()); });
  EXPECT_TRUE(server->start_sync());

  auto client = wirestead::tcp_client("127.0.0.1", port).max_retries(50).build();
  EXPECT_TRUE(client->start_sync());

  const std::string chunk(16 * 1024, 'x');
  size_t sent = 0;
  while (sent < kPayloadBytes) {
    if (client->send_blocking(chunk)) {
      sent += chunk.size();
    } else {
      std::this_thread::sleep_for(1ms);
    }
  }

  for (int i = 0; i < 400; ++i) {
    {
      std::lock_guard<std::mutex> lock(tally->mtx);
      if (tally->total >= kPayloadBytes) break;
    }
    std::this_thread::sleep_for(10ms);
  }

  client->stop();
  server->stop();

  std::lock_guard<std::mutex> lock(tally->mtx);
  return TransferResult{tally->total, tally->largest_chunk};
}

}  // namespace

// The whole point of making the read buffer configurable: a bulk transfer
// should be delivered in chunks larger than the old fixed 4 KiB array, which
// is what cuts read completions and callback dispatches.
TEST(ReadBufferSizeIntegrationTest, LargerBufferDeliversLargerChunks) {
  const uint16_t port = test::TestUtils::getAvailableTestPort();
  auto result = run_transfer(port, kLargeReadBuffer);

  EXPECT_EQ(result.total, kPayloadBytes) << "all bytes must still arrive intact";
  EXPECT_GT(result.largest_chunk, base::constants::DEFAULT_READ_BUFFER_SIZE)
      << "a 64 KiB read buffer must be able to deliver more than the old fixed 4 KiB";
  EXPECT_LE(result.largest_chunk, kLargeReadBuffer) << "must never exceed the configured buffer";
}

// The default must keep behaving exactly as before: no chunk larger than the
// 4 KiB the fixed array used to provide.
TEST(ReadBufferSizeIntegrationTest, DefaultBufferCapsChunksAtTheOldSize) {
  const uint16_t port = test::TestUtils::getAvailableTestPort();
  auto result = run_transfer(port, base::constants::DEFAULT_READ_BUFFER_SIZE);

  EXPECT_EQ(result.total, kPayloadBytes);
  EXPECT_LE(result.largest_chunk, base::constants::DEFAULT_READ_BUFFER_SIZE);
}

// An out-of-range request must be rejected before changing the client.
TEST(ReadBufferSizeIntegrationTest, OversizedRequestIsRejectedBeforeStart) {
  const uint16_t port = test::TestUtils::getAvailableTestPort();
  wrapper::TcpClient client("127.0.0.1", port);
  EXPECT_THROW(client.read_buffer_size(base::constants::MAX_READ_BUFFER_SIZE * 8), std::invalid_argument);
}

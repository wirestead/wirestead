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

#include <functional>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "unilink/base/visibility.hpp"
#include "unilink/diagnostics/logger.hpp"
#include "unilink/framer/iframer.hpp"
#include "unilink/wrapper/context.hpp"
#include "unilink/wrapper/runtime_stats.hpp"

namespace unilink {
namespace wrapper {

/**
 * @brief Common interface for 1:1 point-to-point communication (e.g., TcpClient, Serial, Udp)
 */
class UNILINK_API ChannelInterface {
 public:
  using MessageHandler = std::function<void(const MessageContext&)>;
  using BatchMessageHandler = std::function<void(const std::vector<MessageContext>&)>;
  using ConnectionHandler = std::function<void(const ConnectionContext&)>;
  using ErrorHandler = std::function<void(const ErrorContext&)>;

  virtual ~ChannelInterface() = default;

  // Lifecycle

  /**
   * @brief Start the channel asynchronously.
   *
   * @return A future that resolves to true when the channel is connected/listening,
   *         or false if startup failed (e.g. connection refused, max retries exhausted).
   *         On false, the registered on_error() callback will have been invoked with
   *         the specific failure reason. This future always resolves - including on a
   *         restart after stop() (#444) - never blocks indefinitely.
   */
  [[nodiscard]] virtual std::future<bool> start() = 0;

  /**
   * @brief Synchronously start the channel and wait for the result.
   *
   * @return true if connected/listening, false on failure. On false, the registered
   *         on_error() callback will have been invoked with the specific failure reason.
   */
  [[nodiscard]] virtual bool start_sync() { return start().get(); }

  /**
   * @brief Stop the channel and block until all pending async operations are cancelled.
   *
   * Safe to call from any thread. After stop() returns, no further callbacks will fire
   * and it is safe to destroy the object. Calling stop() more than once is a no-op.
   *
   * Restart contract (#444): stop() fully tears down the underlying transport
   * (socket/serial port, queues, timers) rather than leaving it in a reusable
   * half-alive state. Every on_*() callback and every config setter ever
   * called on this wrapper remains in force across any number of stop()/
   * start() cycles - a subsequent start() reconstructs the transport from
   * this wrapper's retained configuration and re-registers all callbacks
   * automatically, including config changes made while stopped. Session/
   * client-id state and stats() reset on restart. Callers never need to
   * re-register callbacks or reconfigure after a stop()/start() cycle.
   */
  virtual void stop() = 0;
  virtual bool connected() const = 0;
  virtual RuntimeStats stats() const = 0;
  virtual void reset_stats() = 0;

  // Transmission
  //
  // Strategy-aware API (recommended):
  //
  //   send() / send_line()
  //     Behaviour depends on the configured backpressure strategy:
  //       BestEffort — non-blocking; drops data when the send queue is full.
  //       Reliable   — blocks the calling thread until queue pressure is relieved,
  //                    then enqueues. Never drops due to backpressure alone.
  //
  // Explicit API (escape hatch):
  //
  //   try_send() / try_send_line()
  //     Always non-blocking and always rejects a full/backpressured queue,
  //     regardless of strategy. Reliable channels do not enqueue into pending_
  //     for try_send().
  //     Rejects payloads that would exceed the current non-blocking queue
  //     threshold; payloads below MAX_BUFFER_SIZE can still be rejected when
  //     they exceed the current backpressure high-water budget.
  //     Use when you need BestEffort behaviour on a Reliable channel for a specific
  //     call (e.g. fire-and-forget heartbeat).
  //
  //   send_blocking() / send_line_blocking()
  //     Always blocks until queue pressure is relieved, regardless of strategy.
  //     This call may block indefinitely while the channel remains active and
  //     backpressure does not clear. stop() from another thread is expected to
  //     unblock waiting senders.

  /**
   * @brief Enqueue data for transmission, honouring the backpressure strategy.
   *
   * BestEffort: non-blocking, drops if the send queue is full.
   * Reliable:   blocks until queue pressure is relieved, then enqueues.
   *
   * @return true  Data was accepted into the send queue.
   * @return false Data was dropped (not connected, or BestEffort queue full).
   */
  virtual bool send(std::string_view data) = 0;

  /**
   * @brief Enqueue a line (data + "\n") for transmission, honouring the backpressure strategy.
   * @return true  Data was accepted. @return false Data was dropped.
   */
  virtual bool send_line(std::string_view line) = 0;

  /**
   * @brief Block the calling thread until queue pressure is relieved, then enqueue.
   *
   * Ignores the configured strategy — always blocks. Prefer send() unless you need
   * to override BestEffort behaviour for a specific call.
   * This call may block indefinitely while the channel remains active and
   * backpressure does not clear. stop() from another thread is expected to unblock
   * waiting senders.
   *
   * @return true Data was accepted. @return false Channel stopped while waiting.
   */
  virtual bool send_blocking(std::string_view data) = 0;

  /**
   * @brief Blocking variant of send_line(). Always blocks regardless of strategy.
   *
   * This call may block indefinitely while the channel remains active and
   * backpressure does not clear. stop() from another thread is expected to unblock
   * waiting senders.
   *
   * @return true Data was accepted. @return false Channel stopped while waiting.
   */
  virtual bool send_line_blocking(std::string_view line) = 0;

  /**
   * @brief Non-blocking send that always drops on a full queue, ignoring strategy.
   *
   * Use as an escape hatch when you need drop-on-full behaviour on a Reliable channel.
   * Rejects payloads that would exceed the current non-blocking queue threshold.
   * A payload may be rejected even if it is below MAX_BUFFER_SIZE when it is larger
   * than the current backpressure high-water budget. Use send() or send_blocking()
   * when Reliable enqueue semantics are required for large payloads.
   *
   * @return true Data was accepted. @return false Dropped (not connected or queue full).
   */
  virtual bool try_send(std::string_view data) = 0;

  /**
   * @brief Non-blocking send_line that always drops on a full queue, ignoring strategy.
   *
   * Uses the same non-blocking queue threshold policy as try_send().
   *
   * @return true Data was accepted. @return false Dropped.
   */
  virtual bool try_send_line(std::string_view line) = 0;

  /**
   * @brief Enqueue a vector payload by transferring ownership, honouring the backpressure strategy.
   *
   * After this call, the caller must treat the moved-from vector as consumed regardless
   * of the return value. Existing string_view send APIs remain available for borrowed data.
   *
   * @return true Data was accepted. @return false Data was dropped or rejected.
   */
  virtual bool send_move(std::vector<uint8_t>&& data) = 0;

  /**
   * @brief Non-blocking ownership-transfer send.
   *
   * Always returns without waiting for backpressure. The moved-from vector is consumed
   * regardless of the return value.
   * Rejects payloads that would exceed the current non-blocking queue threshold,
   * even below MAX_BUFFER_SIZE when they exceed the current high-water budget.
   *
   * @return true Data was accepted. @return false Data was dropped or rejected.
   */
  virtual bool try_send_move(std::vector<uint8_t>&& data) = 0;

  /**
   * @brief Enqueue an immutable shared vector payload, honouring the backpressure strategy.
   *
   * The shared buffer must be non-null and non-empty.
   *
   * @return true Data was accepted. @return false Data was dropped or rejected.
   */
  virtual bool send_shared(std::shared_ptr<const std::vector<uint8_t>> data) = 0;

  /**
   * @brief Non-blocking shared-buffer send.
   *
   * The shared buffer must be non-null and non-empty.
   * Rejects payloads that would exceed the current non-blocking queue threshold,
   * even below MAX_BUFFER_SIZE when they exceed the current high-water budget.
   *
   * @return true Data was accepted. @return false Data was dropped or rejected.
   */
  virtual bool try_send_shared(std::shared_ptr<const std::vector<uint8_t>> data) = 0;

  // Event handlers
  virtual ChannelInterface& on_data(MessageHandler handler) = 0;

  /** @brief Register a callback for batched data reception */
  virtual ChannelInterface& on_data_batch(BatchMessageHandler handler) = 0;

  virtual ChannelInterface& on_connect(ConnectionHandler handler) = 0;
  virtual ChannelInterface& on_disconnect(ConnectionHandler handler) = 0;
  virtual ChannelInterface& on_error(ErrorHandler handler) = 0;

  /**
   * @brief Register a callback to be notified when send queue congestion changes.
   * @param handler Callback receiving the current number of queued bytes.
   *
   * Default implementation is a no-op. Concrete implementations that support
   * backpressure reporting must override this method; otherwise the handler
   * is silently discarded.
   */
  virtual ChannelInterface& on_backpressure(std::function<void(size_t)> handler) {
    if (handler) {
      UNILINK_LOG_WARNING("channel_interface", "on_backpressure",
                          "Backpressure reporting is not supported by this channel; handler is discarded.");
    }
    return *this;
  }

  /**
   * @brief Set a message framer for this channel.
   * @param framer The framer instance to use.
   */
  virtual ChannelInterface& framer(std::unique_ptr<framer::IFramer> framer) = 0;

  /**
   * @brief Set a handler for complete messages extracted by the framer.
   * @param handler The callback for framed messages.
   */
  virtual ChannelInterface& on_message(MessageHandler handler) = 0;

  /** @brief Register a callback for batched framed message reception */
  virtual ChannelInterface& on_message_batch(BatchMessageHandler handler) = 0;

  // Management
  virtual ChannelInterface& auto_start(bool manage = true) = 0;
};

}  // namespace wrapper
}  // namespace unilink

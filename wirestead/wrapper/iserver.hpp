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
#include <string_view>
#include <vector>

#include "wirestead/base/visibility.hpp"
#include "wirestead/diagnostics/logger.hpp"
#include "wirestead/framer/iframer.hpp"
#include "wirestead/wrapper/context.hpp"
#include "wirestead/wrapper/runtime_stats.hpp"

namespace wirestead {
namespace wrapper {

/**
 * @brief Interface for 1:N server communication (e.g., TcpServer)
 */
class WIRESTEAD_API ServerInterface {
 public:
  using MessageHandler = std::function<void(const MessageContext&)>;
  using BatchMessageHandler = std::function<void(const std::vector<MessageContext>&)>;
  using ConnectionHandler = std::function<void(const ConnectionContext&)>;
  using ErrorHandler = std::function<void(const ErrorContext&)>;
  using FramerFactory = std::function<std::unique_ptr<framer::IFramer>()>;

  virtual ~ServerInterface() = default;

  // Lifecycle
  /**
   * @brief Start the server asynchronously.
   *
   * @return A future that resolves to true when listening, or false on failure
   *         (e.g. bind failure, port already in use). This future always
   *         resolves - including on a restart after stop() (#444) - never
   *         blocks indefinitely.
   */
  [[nodiscard]] virtual std::future<bool> start() = 0;

  /**
   * @brief Synchronously start the channel/server and wait for the result.
   */
  [[nodiscard]] virtual bool start_sync() { return start().get(); }

  /**
   * @brief Stop the server and block until all active sessions are closed.
   *
   * Safe to call from any thread. After stop() returns, no further callbacks will fire
   * and it is safe to destroy the object. Calling stop() more than once is a no-op.
   *
   * Restart contract (#444): stop() fully tears down the underlying transport
   * (acceptor, sessions, timers) rather than leaving it in a reusable
   * half-alive state. Every on_*() callback and every config setter ever
   * called on this wrapper remains in force across any number of stop()/
   * start() cycles - a subsequent start() reconstructs the transport from
   * this wrapper's retained configuration and re-registers all callbacks
   * automatically, including config changes made while stopped. Connected-
   * client state and stats() reset on restart. Callers never need to
   * re-register callbacks or reconfigure after a stop()/start() cycle.
   */
  virtual void stop() = 0;
  virtual bool listening() const = 0;
  virtual RuntimeStats stats() const = 0;
  virtual void reset_stats() = 0;

  // Transmission
  //
  // Strategy-aware API (recommended):
  //
  //   send_to() / broadcast()
  //     Behaviour depends on the configured backpressure strategy:
  //       BestEffort — non-blocking; drops data when the target client's queue is full.
  //       Reliable   — blocks the calling thread until queue pressure is relieved,
  //                    then enqueues for send_to().
  //     broadcast() always performs non-blocking fan-out to connected clients.
  //     It does not wait for one slow client to relieve backpressure.
  //
  // Explicit API (escape hatch):
  //
  //   try_send_to() / try_broadcast()
  //     Always non-blocking and always drops on full queue, regardless of strategy.
  //     Rejects payloads that would exceed the current non-blocking queue
  //     threshold; payloads below MAX_BUFFER_SIZE can still be rejected when
  //     they exceed the current backpressure high-water budget.
  //
  //   send_to_blocking()
  //     Always blocks until queue pressure is relieved, regardless of strategy.
  //     This call may block indefinitely while the server remains active and
  //     backpressure does not clear. stop() from another thread is expected to
  //     unblock waiting senders.

  /**
   * @brief Send to a specific client, honouring the backpressure strategy.
   *
   * BestEffort: non-blocking, drops if the client's send queue is full.
   * Reliable:   blocks until queue pressure is relieved, then enqueues.
   *
   * @return true Data was accepted. @return false Dropped or client not found.
   */
  virtual bool send_to(ClientId client_id, std::string_view data) = 0;

  /**
   * @brief Send to all connected clients using non-blocking fan-out.
   *
   * Does not wait for slow clients to relieve backpressure. Use send_to_blocking()
   * for strict per-client blocking delivery.
   *
   * @return true At least one client accepted the data.
   */
  virtual bool broadcast(std::string_view data) = 0;

  /**
   * @brief Block until queue pressure is relieved, then send to a client. Ignores strategy.
   *
   * This call may block indefinitely while the server remains active and
   * backpressure does not clear. stop() from another thread is expected to unblock
   * waiting senders.
   *
   * @return true Data was accepted. @return false Server stopped while waiting.
   */
  virtual bool send_to_blocking(ClientId client_id, std::string_view data) = 0;

  /**
   * @brief Non-blocking send_to that always drops on a full queue, ignoring strategy.
   *
   * Use as an escape hatch when you need drop-on-full behaviour on a Reliable channel.
   * Rejects payloads that would exceed the current non-blocking queue threshold.
   * A payload may be rejected even if it is below MAX_BUFFER_SIZE when it is larger
   * than the current backpressure high-water budget. Use send_to() or
   * send_to_blocking() when Reliable enqueue semantics are required for large
   * payloads.
   *
   * @return true Data was accepted. @return false Dropped or client not found.
   */
  virtual bool try_send_to(ClientId client_id, std::string_view data) = 0;

  /**
   * @brief Non-blocking broadcast that always drops on full queues, ignoring strategy.
   *
   * Uses the same non-blocking queue threshold policy as try_send_to().
   *
   * @return true At least one client accepted the data.
   */
  virtual bool try_broadcast(std::string_view data) = 0;

  /**
   * @brief Send a line (data + "\n") to all clients, honouring the backpressure strategy.
   */
  virtual bool broadcast_line(std::string_view line) = 0;

  /**
   * @brief Send a line (data + "\n") to a specific client, honouring the backpressure strategy.
   */
  virtual bool send_to_line(ClientId client_id, std::string_view line) = 0;

  /**
   * @brief Non-blocking broadcast_line that always drops on a full queue, ignoring strategy.
   */
  virtual bool try_broadcast_line(std::string_view line) = 0;

  /**
   * @brief Non-blocking send_to_line that always drops on a full queue, ignoring strategy.
   */
  virtual bool try_send_to_line(ClientId client_id, std::string_view line) = 0;

  // Event handlers
  virtual ServerInterface& on_connect(ConnectionHandler handler) = 0;
  virtual ServerInterface& on_disconnect(ConnectionHandler handler) = 0;
  virtual ServerInterface& on_data(MessageHandler handler) = 0;

  /** @brief Register a callback for batched data reception */
  virtual ServerInterface& on_data_batch(BatchMessageHandler handler) = 0;

  virtual ServerInterface& on_error(ErrorHandler handler) = 0;

  /**
   * @brief Register a callback to be notified when send queue congestion changes.
   * @param handler Callback receiving the current number of queued bytes.
   *
   * Default implementation is a no-op. Concrete implementations that support
   * backpressure reporting must override this method; otherwise the handler
   * is silently discarded.
   */
  virtual ServerInterface& on_backpressure(std::function<void(size_t)> handler) {
    if (handler) {
      WIRESTEAD_LOG_WARNING("server_interface", "on_backpressure",
                            "Backpressure reporting is not supported by this server; handler is discarded.");
    }
    return *this;
  }

  /**
   * @brief Set a factory function to create a new framer for each client connection.
   * @param factory Function that returns a unique_ptr to a new framer.
   */
  virtual ServerInterface& framer(FramerFactory factory) = 0;

  /**
   * @brief Set a handler for complete messages extracted by the framer.
   * @param handler callback taking MessageContext (where data is the framed payload).
   */
  virtual ServerInterface& on_message(MessageHandler handler) = 0;

  /** @brief Register a callback for batched framed message reception */
  virtual ServerInterface& on_message_batch(BatchMessageHandler handler) = 0;

  // Management
  virtual ServerInterface& auto_start(bool manage = true) = 0;
  virtual size_t client_count() const = 0;
  virtual std::vector<ClientId> connected_clients() const = 0;
};

}  // namespace wrapper
}  // namespace wirestead

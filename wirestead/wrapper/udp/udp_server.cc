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

#include "wirestead/wrapper/udp/udp_server.hpp"

#include <spdlog/fmt/fmt.h>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <stop_token>
#include <thread>
#include <unordered_map>
#include <vector>

#include "wirestead/base/common.hpp"
#include "wirestead/concurrency/io_thread_hook.hpp"
#include "wirestead/factory/channel_factory.hpp"
#include "wirestead/transport/udp/detail/write_wait.hpp"
#include "wirestead/transport/udp/udp.hpp"
#include "wirestead/wrapper/callback_guard.hpp"
#include "wirestead/wrapper/error_context_builder.hpp"
#include "wirestead/wrapper/send_validation.hpp"

namespace wirestead {
namespace wrapper {

namespace {
// std::hash<boost::asio::ip::udp::endpoint> is not available before Boost 1.74.
// Provide a portable hash by combining the raw address bytes and port.
struct UdpEndpointHash {
  std::size_t operator()(const boost::asio::ip::udp::endpoint& ep) const noexcept {
    std::size_t seed = 0;
    auto combine = [&](std::size_t v) { seed ^= v + 0x9e3779b9u + (seed << 6) + (seed >> 2); };
    if (ep.address().is_v4()) {
      for (auto byte : ep.address().to_v4().to_bytes()) {
        combine(std::hash<unsigned char>{}(byte));
      }
    } else {
      for (auto byte : ep.address().to_v6().to_bytes()) {
        combine(std::hash<unsigned char>{}(byte));
      }
    }
    combine(std::hash<unsigned short>{}(ep.port()));
    return seed;
  }
};
}  // namespace

struct UdpServer::Impl : public std::enable_shared_from_this<Impl> {
  config::UdpConfig cfg;
  std::shared_ptr<transport::UdpChannel> channel;
  std::shared_ptr<boost::asio::io_context> external_ioc;
  std::atomic<bool> use_external_context{false};
  std::atomic<bool> manage_external_context{false};
  std::jthread external_thread;
  std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> work_guard;

  mutable std::shared_mutex mutex;
  std::mutex bp_mutex_;
  std::condition_variable bp_cv_;
  // D-1: admission gate for this object's user callbacks. Admission, the
  // running count and the closed flag are one decision, so a stop() cannot
  // observe an empty gate while a callback is about to start, and a callback
  // left over from a previous run is refused after a restart.
  detail::CallbackGate callback_gate_;
  std::mutex stop_finalize_mutex_;
  bool stop_requested_ = false;
  std::atomic<unsigned> stop_callers_{0};
  std::atomic<uint64_t> callback_generation_{0};

  // True when this thread is one the target's shutdown needs: a callback of
  // this object, or any thread currently running the external io_context this
  // channel was built on (which a callback of another channel sharing it is).
  // Such a caller requests the shutdown and returns; it cannot wait for work
  // its own thread has to perform.
  bool shutdown_needs_this_thread() const {
    if (callback_gate_.active_on_this_thread()) return true;
    if (external_ioc && external_ioc->get_executor().running_in_this_thread()) return true;
    std::shared_lock<std::shared_mutex> lock(mutex);
    if (!channel) return false;
    const auto executor = channel->get_executor();
    using IoExecutor = boost::asio::io_context::executor_type;
    if (auto* io = executor.target<IoExecutor>()) return io->running_in_this_thread();
    if (auto* strand = executor.target<boost::asio::strand<IoExecutor>>())
      return strand->get_inner_executor().running_in_this_thread();
    return false;
  }

  std::vector<std::promise<bool>> pending_promises;
  std::atomic<bool> started{false};
  std::atomic<bool> is_listening{false};

  // Virtual Session Management
  struct SessionEntry {
    boost::asio::ip::udp::endpoint endpoint;
    std::shared_ptr<transport::detail::UdpWriteWait> wait;
    std::shared_ptr<framer::IFramer> framer;
    std::chrono::steady_clock::time_point last_seen;
  };
  ClientId next_client_id{1};
  std::unordered_map<boost::asio::ip::udp::endpoint, ClientId, UdpEndpointHash> endpoint_to_id;
  std::unordered_map<ClientId, SessionEntry> sessions;
  std::chrono::milliseconds session_timeout{0};  // 0 = disabled
  std::unique_ptr<boost::asio::steady_timer> reaper_timer;
  std::atomic<bool> auto_start{false};
  std::atomic<bool> client_limit_enabled{false};
  std::atomic<size_t> max_clients_limit{0};

  ConnectionHandler on_connect{nullptr};
  ConnectionHandler on_disconnect{nullptr};
  // Shared snapshots: the strand copies one out per received datagram, and a
  // std::function copy allocates whenever the user handler outgrows its
  // small-object buffer. See interface::SharedCallback.
  interface::SharedCallback<MessageHandler> on_data;
  interface::SharedCallback<BatchMessageHandler> on_data_batch_;
  ErrorHandler on_error{nullptr};
  std::function<void(size_t)> bp_handler{nullptr};
  FramerFactory framer_factory{nullptr};
  interface::SharedCallback<MessageHandler> on_message;
  interface::SharedCallback<BatchMessageHandler> on_message_batch_;

  bool factory_managed_channel_ = true;
  std::shared_ptr<bool> is_alive{std::make_shared<bool>(true)};

  // Batching logic
  std::vector<MessageContext> data_batch_queue_;
  std::vector<MessageContext> message_batch_queue_;
  std::unique_ptr<boost::asio::steady_timer> batch_timer_;
  size_t max_batch_size_ = 100;
  std::chrono::milliseconds max_batch_latency_{1};

  explicit Impl(const config::UdpConfig& config) : cfg(config) {}
  Impl(const config::UdpConfig& config, std::shared_ptr<boost::asio::io_context> ioc)
      : cfg(config), external_ioc(std::move(ioc)), use_external_context(external_ioc != nullptr) {}
  explicit Impl(std::shared_ptr<interface::Channel> ch)
      : channel(std::dynamic_pointer_cast<transport::UdpChannel>(ch)), factory_managed_channel_(false) {
    // #450: setup_internal_handlers() captures weak_from_this() - calling it
    // from inside this constructor would capture an empty weak_ptr, since
    // enable_shared_from_this isn't wired up until make_shared() finishes
    // constructing the object. Deferred to UdpServer's own constructor,
    // which runs after impl_ is a fully-formed shared_ptr<Impl>.
  }

  ~Impl() {
    try {
      stop();
    } catch (...) {
    }
  }

  void fulfill_all_locked(bool value) {
    for (auto& promise : pending_promises) {
      try {
        promise.set_value(value);
      } catch (...) {
      }
    }
    pending_promises.clear();
  }

  void flush_batches(uint64_t generation) {
    auto lease = callback_gate_.enter(generation);
    if (!lease.admitted()) return;
    std::unique_lock<std::shared_mutex> lock(mutex);
    if (!data_batch_queue_.empty()) {
      auto handler = on_data_batch_;
      auto batch = std::move(data_batch_queue_);
      data_batch_queue_.clear();
      if (handler) {
        lock.unlock();
        detail::invoke_user_callback("udp_server", "on_data_batch", handler, batch);
        lock.lock();
      }
    }
    if (!message_batch_queue_.empty()) {
      auto handler = on_message_batch_;
      auto batch = std::move(message_batch_queue_);
      message_batch_queue_.clear();
      if (handler) {
        lock.unlock();
        detail::invoke_user_callback("udp_server", "on_message_batch", handler, batch);
        lock.lock();
      }
    }
    if (batch_timer_) {
      batch_timer_->cancel();
    }
  }

  void schedule_batch_timer(uint64_t generation) {
    if (!batch_timer_) return;
    batch_timer_->expires_after(max_batch_latency_);
    batch_timer_->async_wait([this, generation, weak_impl = weak_from_this(),
                              alive = std::weak_ptr<bool>(is_alive)](const boost::system::error_code& ec) {
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      auto lock = alive.lock();
      if (!lock || !(*lock)) return;

      if (!ec) {
        flush_batches(generation);
      }
    });
  }

  void schedule_reaper(uint64_t generation) {
    if (!started.load() || !reaper_timer || session_timeout.count() <= 0) return;

    // Run reaper at interval proportional to timeout (min 100ms, max 5s)
    auto interval =
        std::max(std::chrono::milliseconds(100), std::min(std::chrono::milliseconds(5000), session_timeout / 2));

    reaper_timer->expires_after(interval);
    reaper_timer->async_wait([this, generation, weak_impl = weak_from_this(),
                              alive = std::weak_ptr<bool>(is_alive)](const boost::system::error_code& ec) {
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      auto lock = alive.lock();
      if (!lock || !(*lock)) return;

      if (!ec) {
        auto lease = callback_gate_.enter(generation);
        if (!lease.admitted()) return;
        run_reaper();
        std::unique_lock<std::shared_mutex> guard(mutex);
        schedule_reaper(generation);
      }
    });
  }

  void run_reaper() {
    std::vector<std::pair<ClientId, std::string>> to_remove_with_info;
    auto now = std::chrono::steady_clock::now();

    ConnectionHandler disconnect_handler;
    {
      std::unique_lock<std::shared_mutex> lock(mutex);
      if (session_timeout.count() <= 0) return;
      for (auto it = sessions.begin(); it != sessions.end();) {
        if (now - it->second.last_seen > session_timeout) {
          std::string info =
              fmt::format("{}:{}", it->second.endpoint.address().to_string(), it->second.endpoint.port());
          if (channel) channel->end_write_wait(it->second.wait, SendRejection::NotReady);
          bp_cv_.notify_all();
          endpoint_to_id.erase(it->second.endpoint);
          to_remove_with_info.push_back({it->first, info});
          it = sessions.erase(it);
        } else {
          ++it;
        }
      }
      disconnect_handler = on_disconnect;
    }

    // Call disconnect handlers outside the lock
    for (auto const& [id, info] : to_remove_with_info) {
      detail::invoke_user_callback("udp_server", "on_disconnect", disconnect_handler, ConnectionContext(id, info));
    }
  }

  void setup_internal_handlers() {
    if (!channel) return;

    batch_timer_ = std::make_unique<boost::asio::steady_timer>(channel->get_executor());

    std::weak_ptr<Impl> weak_impl = weak_from_this();
    const auto generation = callback_gate_.open_new_generation();
    callback_generation_.store(generation);

    channel->on_bytes_from(
        [this, generation, weak_impl](memory::ConstByteSpan data, const boost::asio::ip::udp::endpoint& ep) {
          // Retain Impl even when an old handler is delayed before admission.
          // A completed stop may leave such a refused handler to return later.
          auto impl_keepalive = weak_impl.lock();
          if (!impl_keepalive) return;
          auto lease = callback_gate_.enter(generation);
          if (!lease.admitted()) return;

          // #449: everything below runs synchronously on this io thread - mark
          // it so a blocking send_to() called from within one of these
          // callbacks fails fast instead of deadlocking.
          detail::CallbackGuard callback_guard;

          ClientId client_id = 0;
          bool is_new = false;
          ConnectionHandler connect_handler_copy{nullptr};

          {
            std::unique_lock<std::shared_mutex> lock(mutex);
            auto it = endpoint_to_id.find(ep);
            if (it == endpoint_to_id.end()) {
              if (client_limit_enabled.load() && sessions.size() >= max_clients_limit.load()) {
                return;
              }
              client_id = next_client_id++;
              endpoint_to_id[ep] = client_id;
              SessionEntry entry;
              entry.endpoint = ep;
              entry.wait = channel->capture_write_wait(false);
              entry.last_seen = std::chrono::steady_clock::now();
              is_new = true;

              // Create framer for new session
              if (framer_factory) {
                auto framer = framer_factory();
                if (framer) {
                  framer->on_message([this, client_id, generation](memory::ConstByteSpan msg) {
                    // #441: snapshot under a shared_lock (pure read), build the
                    // copy before taking the exclusive lock for queue mutation.
                    bool batch_mode;
                    interface::SharedCallback<MessageHandler> on_message_handler;
                    {
                      std::shared_lock<std::shared_mutex> lock(mutex);
                      batch_mode = static_cast<bool>(on_message_batch_);
                      on_message_handler = on_message;
                    }

                    if (batch_mode) {
                      MessageContext ctx(client_id, memory::SafeDataBuffer(msg));
                      interface::SharedCallback<BatchMessageHandler> flush_handler;
                      std::vector<MessageContext> batch;
                      {
                        std::unique_lock<std::shared_mutex> lock(mutex);
                        message_batch_queue_.emplace_back(std::move(ctx));
                        if (message_batch_queue_.size() >= max_batch_size_) {
                          flush_handler = on_message_batch_;
                          batch = std::move(message_batch_queue_);
                          message_batch_queue_.clear();
                        } else if (message_batch_queue_.size() == 1) {
                          schedule_batch_timer(generation);
                        }
                      }
                      detail::invoke_user_callback("udp_server", "on_message_batch", flush_handler, batch);
                      return;
                    }

                    detail::invoke_user_callback("udp_server", "on_message", on_message_handler,
                                                 MessageContext(client_id, msg));
                  });
                  entry.framer = std::move(framer);
                }
              }
              sessions[client_id] = std::move(entry);
            } else {
              client_id = it->second;
              sessions[client_id].last_seen = std::chrono::steady_clock::now();
            }
            connect_handler_copy = on_connect;
          }

          if (is_new) {
            detail::invoke_user_callback(
                "udp_server", "on_connect", connect_handler_copy,
                ConnectionContext(client_id, fmt::format("{}:{}", ep.address().to_string(), ep.port())));
          }

          {
            // #441: snapshot the handler under a shared_lock (not unique_lock) -
            // this is a pure read, matching try_send's locking level so it no
            // longer blocks concurrent sends even briefly.
            bool batch_mode;
            interface::SharedCallback<MessageHandler> data_handler_copy;
            {
              std::shared_lock<std::shared_mutex> lock(mutex);
              batch_mode = static_cast<bool>(on_data_batch_);
              data_handler_copy = on_data;
            }

            if (batch_mode) {
              // #441: build the copy before taking the exclusive lock, so the
              // lock is only held for the queue mutation itself, not the
              // allocation.
              MessageContext ctx(client_id, memory::SafeDataBuffer(data));
              interface::SharedCallback<BatchMessageHandler> flush_handler;
              std::vector<MessageContext> batch;
              {
                std::unique_lock<std::shared_mutex> lock(mutex);
                data_batch_queue_.emplace_back(std::move(ctx));
                if (data_batch_queue_.size() >= max_batch_size_) {
                  flush_handler = on_data_batch_;
                  batch = std::move(data_batch_queue_);
                  data_batch_queue_.clear();
                } else if (data_batch_queue_.size() == 1) {
                  schedule_batch_timer(generation);
                }
              }
              detail::invoke_user_callback("udp_server", "on_data_batch", flush_handler, batch);
            } else {
              detail::invoke_user_callback("udp_server", "on_data", data_handler_copy, MessageContext(client_id, data));
            }
          }

          // Push to framer
          std::shared_ptr<framer::IFramer> target_framer;
          {
            std::shared_lock<std::shared_mutex> lock(mutex);
            auto it = sessions.find(client_id);
            if (it != sessions.end()) {
              target_framer = it->second.framer;
            }
          }
          if (target_framer) {
            target_framer->push_bytes(data);
          }
        });

    channel->on_backpressure([this, generation, weak_impl](size_t queued) {
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      auto lease = callback_gate_.enter(generation);
      if (!lease.admitted()) return;
      bp_cv_.notify_all();
      std::function<void(size_t)> handler;
      {
        std::shared_lock<std::shared_mutex> lock(mutex);
        handler = bp_handler;
      }
      detail::invoke_user_callback("udp_server", "on_backpressure", handler, queued);
    });

    channel->on_state([this, generation, weak_impl](base::LinkState state) {
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      auto lease = callback_gate_.enter(generation);
      if (!lease.admitted()) return;
      ErrorHandler error_handler_copy{nullptr};
      if (state == base::LinkState::Listening || state == base::LinkState::Connected) {
        is_listening.store(true);
        std::unique_lock<std::shared_mutex> lock(mutex);
        fulfill_all_locked(true);
      } else if (state == base::LinkState::Error || state == base::LinkState::Closed ||
                 state == base::LinkState::Idle) {
        is_listening.store(false);
        std::unique_lock<std::shared_mutex> lock(mutex);
        fulfill_all_locked(false);
        if (state == base::LinkState::Error) {
          error_handler_copy = on_error;
        }
      }

      detail::invoke_user_callback("udp_server", "on_error", error_handler_copy,
                                   channel ? detail::build_error_context(*channel, "Server error")
                                           : ErrorContext(ErrorCode::IoError, "Server error"));
    });
  }

  std::future<bool> start() {
    std::unique_lock<std::shared_mutex> lock(mutex);
    stop_requested_ = false;
    if (is_listening.load()) {
      std::promise<bool> p;
      p.set_value(true);
      return p.get_future();
    }

    std::promise<bool> p;
    auto fut = p.get_future();
    pending_promises.emplace_back(std::move(p));

    if (started.exchange(true)) {
      return fut;
    }

    if (!is_alive) is_alive = std::make_shared<bool>(true);
    if (!channel) {
      channel = std::dynamic_pointer_cast<transport::UdpChannel>(factory::ChannelFactory::create(cfg, external_ioc));
    }
    setup_internal_handlers();

    auto channel_copy = channel;
    lock.unlock();
    channel_copy->start();

    lock.lock();
    if (!started.load()) return fut;
    if (use_external_context.load() && manage_external_context.load() && !external_thread.joinable()) {
      if (external_ioc->stopped()) external_ioc->restart();
      work_guard = std::make_unique<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
          external_ioc->get_executor());
      external_thread = std::jthread([ioc = external_ioc](std::stop_token st) {
        wirestead::concurrency::run_io_thread_init();
        try {
          std::stop_callback cb(st, [ioc] { ioc->stop(); });
          ioc->run();
        } catch (...) {
        }
      });
    }

    if (channel && session_timeout.count() > 0) {
      reaper_timer = std::make_unique<boost::asio::steady_timer>(channel->get_executor());
      schedule_reaper(callback_generation_.load());
    }

    return fut;
  }

  void stop() {
    stop_callers_.fetch_add(1);
    struct StopCall {
      std::atomic<unsigned>& callers;
      ~StopCall() { callers.fetch_sub(1); }
    } stop_call{stop_callers_};
    const bool request_only = shutdown_needs_this_thread();
    callback_gate_.close();
    std::shared_ptr<transport::UdpChannel> channel_copy;
    {
      std::unique_lock<std::shared_mutex> lock(mutex);
      stop_requested_ = true;
      if (channel) channel->cancel_write_waits();
      started.store(false);
      is_listening.store(false);
      bp_cv_.notify_all();
      fulfill_all_locked(false);
      channel_copy = channel;
    }
    if (channel_copy) channel_copy->stop();
    if (request_only) return;

    callback_gate_.wait_until_idle();
    std::lock_guard<std::mutex> finalize_lock(stop_finalize_mutex_);
    {
      std::unique_lock<std::shared_mutex> lock(mutex);
      is_alive.reset();
      if (batch_timer_) {
        batch_timer_->cancel();
        batch_timer_.reset();
      }
      if (reaper_timer) {
        reaper_timer->cancel();
        reaper_timer.reset();
      }
      if (channel) {
        channel->on_bytes_from(nullptr);
        channel->on_state(nullptr);
        channel->on_backpressure(nullptr);
      }
      if (factory_managed_channel_) channel.reset();
      data_batch_queue_.clear();
      message_batch_queue_.clear();
      endpoint_to_id.clear();
      sessions.clear();
      next_client_id = 1;
    }
    if (use_external_context && manage_external_context) {
      work_guard.reset();
      if (external_ioc) external_ioc->stop();
      if (external_thread.joinable()) external_thread.join();
    }
  }

  SendResult send_to(ClientId client_id, std::string_view data) {
    if (cfg.backpressure_strategy == base::constants::BackpressureStrategy::Reliable)
      return send_to_blocking(client_id, data);
    return try_send_to(client_id, data, true);
  }

  FanoutResult try_broadcast(std::string_view data, bool append_newline = false) {
    // The shared wrapper lock fixes the virtual-session set for the whole
    // nonblocking traversal, including each endpoint's admission decision.
    std::shared_lock<std::shared_mutex> lock(mutex);
    FanoutResult result;
    const auto size =
        append_newline
            ? (data.size() >= base::constants::MAX_BUFFER_SIZE ? base::constants::MAX_BUFFER_SIZE + 1 : data.size() + 1)
            : data.size();
    const auto validation = detail::validate_payload_size(size, channel ? channel->write_queue_limit() : std::nullopt);
    const auto state = validation.accepted() ? send_state() : validation;
    std::string line;
    if (append_newline && state.accepted() && !sessions.empty()) {
      line = std::string(data) + "\n";
      data = line;
    }
    const auto bytes = base::safe_convert::string_to_bytes(data);
    for (const auto& [id, entry] : sessions) {
      result.add(state.accepted()
                     ? channel->try_write_to({bytes.first, bytes.second}, entry.endpoint,
                                             entry.wait ? std::optional<uint64_t>(entry.wait->sequence) : std::nullopt)
                     : state);
    }
    return result;
  }

  FanoutResult broadcast(std::string_view data) { return try_broadcast(data); }

  static SendResult finish_send(SendResult result) {
    if (auto hook = detail::g_udp_server_send_result_hook.load()) hook(result);
    return result;
  }

  // Caller holds mutex. The native call rechecks socket readiness at admission.
  SendResult send_state() {
    if (stop_callers_.load()) return SendResult::reject(SendRejection::Stopping);
    if (!started.load()) {
      if (stop_requested_) {
        if (!callback_gate_.idle()) return SendResult::reject(SendRejection::Stopping);
        if (channel) {
          const auto state = channel->write_state(false);
          if (!state.accepted() && state.reason() == SendRejection::Stopping) return state;
        }
      }
      return SendResult::reject(SendRejection::NotStarted);
    }
    if (!channel) return SendResult::reject(SendRejection::NotReady);
    return channel->write_state(false);
  }

  SendResult try_send_to(ClientId client_id, std::string_view data, bool best_effort_send = false) {
    const auto result = [&]() -> SendResult {
      std::shared_lock<std::shared_mutex> lock(mutex);
      auto validation =
          detail::validate_payload_size(data.size(), channel ? channel->write_queue_limit() : std::nullopt);
      if (!validation.accepted()) return validation;
      auto state = send_state();
      if (!state.accepted()) return state;
      auto it = sessions.find(client_id);
      if (it == sessions.end()) return SendResult::reject(SendRejection::NotReady);
      auto bytes = base::safe_convert::string_to_bytes(data);
      auto admitted = channel->try_write_to({bytes.first, bytes.second}, it->second.endpoint);
      if (best_effort_send && !admitted.accepted() && admitted.reason() == SendRejection::WouldBlock)
        return SendResult::reject(SendRejection::QueueFull);
      return admitted;
    }();
    return finish_send(result);
  }

  SendResult send_to_blocking(ClientId client_id, std::string_view data) {
    const auto result = [&]() -> SendResult {
      std::shared_ptr<transport::UdpChannel> native;
      std::shared_ptr<transport::detail::UdpWriteWait> wait;
      uint64_t generation;
      {
        std::shared_lock<std::shared_mutex> lock(mutex);
        auto validation =
            detail::validate_payload_size(data.size(), channel ? channel->write_queue_limit() : std::nullopt);
        if (!validation.accepted()) return validation;
        auto state = send_state();
        if (!state.accepted()) return state;
        auto it = sessions.find(client_id);
        if (it == sessions.end() || !it->second.wait) return SendResult::reject(SendRejection::NotReady);
        native = channel;
        wait = it->second.wait;
        generation = callback_generation_.load();
      }
      for (int attempt = 0; attempt < 5; ++attempt) {
        std::unique_lock<std::mutex> bp_lock(bp_mutex_);
        auto outcome = native->poll_write_wait(wait);
        if (!outcome) {
          if (detail::in_data_callback()) return SendResult::reject(SendRejection::WouldBlock);
          if (auto hook = detail::g_udp_capacity_wait_hook.load()) hook();
          while (!bp_cv_.wait_for(bp_lock, std::chrono::milliseconds(50), [&] {
            outcome = native->poll_write_wait(wait);
            return outcome.has_value();
          })) {
          }
          if (auto hook = detail::g_udp_capacity_wait_result_hook.load()) hook(*outcome);
        }
        if (!outcome->accepted()) return *outcome;
        bp_lock.unlock();
        std::shared_lock<std::shared_mutex> lock(mutex);
        auto state = send_state();
        if (!state.accepted()) return state;
        auto it = sessions.find(client_id);
        if (callback_generation_.load() != generation || it == sessions.end() || it->second.wait != wait)
          return SendResult::reject(SendRejection::NotReady);
        auto bytes = base::safe_convert::string_to_bytes(data);
        const auto admitted = native->write_to({bytes.first, bytes.second}, it->second.endpoint, wait->sequence);
        if (admitted.accepted() || admitted.reason() != SendRejection::WouldBlock) return admitted;
        if (detail::in_data_callback()) return admitted;
      }
      return SendResult::reject(SendRejection::WouldBlock);
    }();
    return finish_send(result);
  }

  RuntimeStats stats() const {
    std::shared_lock<std::shared_mutex> lock(mutex);
    return channel ? channel->stats() : RuntimeStats{};
  }

  void reset_stats() {
    std::shared_lock<std::shared_mutex> lock(mutex);
    if (channel) channel->reset_stats();
  }
};

UdpServer::UdpServer(uint16_t port) {
  config::UdpConfig cfg;
  cfg.local_port = port;
  impl_ = std::make_shared<Impl>(cfg);
}

UdpServer::UdpServer(const config::UdpConfig& cfg) : impl_(std::make_shared<Impl>(cfg)) {}

UdpServer::UdpServer(const config::UdpConfig& cfg, std::shared_ptr<boost::asio::io_context> ioc)
    : impl_(std::make_shared<Impl>(cfg, ioc)) {}

UdpServer::UdpServer(std::shared_ptr<interface::Channel> ch) : impl_(std::make_shared<Impl>(std::move(ch))) {
  impl_->setup_internal_handlers();
}

UdpServer::~UdpServer() = default;

UdpServer::UdpServer(UdpServer&&) noexcept = default;
UdpServer& UdpServer::operator=(UdpServer&&) noexcept = default;

std::future<bool> UdpServer::start() { return impl_->start(); }
void UdpServer::stop() { impl_->stop(); }
bool UdpServer::listening() const { return impl_->is_listening.load(); }
RuntimeStats UdpServer::stats() const { return impl_->stats(); }
void UdpServer::reset_stats() { impl_->reset_stats(); }

FanoutResult UdpServer::broadcast(std::string_view data) { return impl_->broadcast(data); }
FanoutResult UdpServer::try_broadcast(std::string_view data) { return impl_->try_broadcast(data); }
SendResult UdpServer::send_to(ClientId client_id, std::string_view data) { return impl_->send_to(client_id, data); }
SendResult UdpServer::try_send_to(ClientId client_id, std::string_view data) {
  return impl_->try_send_to(client_id, data);
}

SendResult UdpServer::send_to_blocking(ClientId client_id, std::string_view data) {
  return impl_->send_to_blocking(client_id, data);
}

FanoutResult UdpServer::broadcast_line(std::string_view line) { return impl_->try_broadcast(line, true); }
SendResult UdpServer::send_to_line(ClientId client_id, std::string_view line) {
  return send_to(client_id, std::string(line) + "\n");
}
FanoutResult UdpServer::try_broadcast_line(std::string_view line) { return impl_->try_broadcast(line, true); }
SendResult UdpServer::try_send_to_line(ClientId client_id, std::string_view line) {
  return try_send_to(client_id, std::string(line) + "\n");
}

UdpServer& UdpServer::on_connect(ConnectionHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->on_connect = std::move(h);
  return *this;
}

UdpServer& UdpServer::on_disconnect(ConnectionHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->on_disconnect = std::move(h);
  return *this;
}

UdpServer& UdpServer::on_data(MessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->on_data = interface::share_callback(std::move(h));
  return *this;
}

UdpServer& UdpServer::on_data_batch(BatchMessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->on_data_batch_ = interface::share_callback(std::move(h));
  return *this;
}

UdpServer& UdpServer::on_error(ErrorHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->on_error = std::move(h);
  return *this;
}

UdpServer& UdpServer::framer(FramerFactory factory) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->framer_factory = std::move(factory);
  return *this;
}

UdpServer& UdpServer::on_message(MessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->on_message = interface::share_callback(std::move(h));
  return *this;
}

UdpServer& UdpServer::on_message_batch(BatchMessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->on_message_batch_ = interface::share_callback(std::move(h));
  return *this;
}

size_t UdpServer::client_count() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex);
  return impl_->endpoint_to_id.size();
}

std::vector<ClientId> UdpServer::connected_clients() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex);
  std::vector<ClientId> ids;
  ids.reserve(impl_->sessions.size());
  for (const auto& [id, entry] : impl_->sessions) {
    ids.push_back(id);
  }
  return ids;
}

UdpServer& UdpServer::auto_start(bool m) {
  impl_->auto_start.store(m);
  if (impl_->auto_start.load() && !impl_->started.load()) {
    start();
  }
  return *this;
}

UdpServer& UdpServer::bind_address(const std::string& address) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->cfg.bind_address = address;
  return *this;
}

UdpServer& UdpServer::idle_timeout(std::chrono::milliseconds timeout) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->session_timeout = timeout;
  if (impl_->session_timeout.count() <= 0) {
    if (impl_->reaper_timer) {
      impl_->reaper_timer->cancel();
      impl_->reaper_timer.reset();
    }
    return *this;
  }
  if (impl_->started.load() && impl_->channel && !impl_->reaper_timer) {
    impl_->reaper_timer = std::make_unique<boost::asio::steady_timer>(impl_->channel->get_executor());
    impl_->schedule_reaper(impl_->callback_generation_.load());
  }
  return *this;
}

UdpServer& UdpServer::on_backpressure(std::function<void(size_t)> handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->bp_handler = std::move(handler);
  return *this;
}

UdpServer& UdpServer::max_clients(size_t max) {
  if (max == 0) {
    impl_->client_limit_enabled.store(false);
    impl_->max_clients_limit.store(0);
  } else {
    impl_->client_limit_enabled.store(true);
    impl_->max_clients_limit.store(max);
  }
  return *this;
}

UdpServer& UdpServer::backpressure_threshold(size_t threshold) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->cfg.backpressure_threshold = threshold;
  return *this;
}

UdpServer& UdpServer::backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->cfg.backpressure_strategy = strategy;
  if (impl_->channel) {
    impl_->channel->set_backpressure_strategy(strategy);
  }
  return *this;
}

size_t UdpServer::backpressure_threshold() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex);
  return impl_->cfg.backpressure_threshold;
}

base::constants::BackpressureStrategy UdpServer::backpressure_strategy() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex);
  return impl_->cfg.backpressure_strategy;
}

UdpServer& UdpServer::send_buffer_size(size_t bytes) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->cfg.send_buffer_size = bytes;
  return *this;
}

UdpServer& UdpServer::receive_buffer_size(size_t bytes) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->cfg.receive_buffer_size = bytes;
  return *this;
}

UdpServer& UdpServer::manage_external_context(bool m) {
  impl_->manage_external_context.store(m);
  return *this;
}

UdpServer& UdpServer::batch_size(size_t size) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->max_batch_size_ = size;
  return *this;
}

UdpServer& UdpServer::batch_latency(std::chrono::milliseconds latency) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex);
  impl_->max_batch_latency_ = latency;
  return *this;
}

}  // namespace wrapper
}  // namespace wirestead

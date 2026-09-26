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

#include "wirestead/wrapper/tcp_server/tcp_server.hpp"

#include <atomic>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <unordered_map>
#include <vector>

#include "wirestead/base/common.hpp"
#include "wirestead/concurrency/io_thread_hook.hpp"
#include "wirestead/config/tcp_server_config.hpp"
#include "wirestead/config/validation.hpp"
#include "wirestead/factory/channel_factory.hpp"
#include "wirestead/transport/tcp_server/tcp_server.hpp"
#include "wirestead/util/input_validator.hpp"
#include "wirestead/wrapper/callback_guard.hpp"
#include "wirestead/wrapper/error_context_builder.hpp"
#include "wirestead/wrapper/send_retry.hpp"
#include "wirestead/wrapper/send_validation.hpp"
#include "wirestead/wrapper/session_batch.hpp"

namespace wirestead {
namespace wrapper {

struct TcpServer::Impl : public std::enable_shared_from_this<Impl> {
  mutable std::shared_mutex mutex_;
  std::mutex bp_mutex_;
  std::condition_variable bp_cv_;
  // D-1: admission gate for this object's user callbacks, across every
  // session. Admission and the running count are one decision under the gate's
  // own lock, and a new run refuses callbacks left over from the previous one.
  detail::CallbackGate callback_gate_;
  std::mutex stop_finalize_mutex_;
  bool injected_channel_ = false;
  std::atomic<uint64_t> callback_generation_{0};
  bool stop_requested_ = false;
  std::atomic<unsigned> stop_callers_{0};

  // A thread the shutdown needs: one running a callback of this object, or one
  // currently running the external io_context this server was built on - which
  // a callback of another channel sharing that context is.
  bool shutdown_needs_this_thread() const {
    if (callback_gate_.active_on_this_thread()) return true;
    if (external_ioc_ && external_ioc_->get_executor().running_in_this_thread()) return true;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (!channel_) return false;
    const auto executor = channel_->get_executor();
    using IoExecutor = boost::asio::io_context::executor_type;
    if (auto* io = executor.target<IoExecutor>()) return io->running_in_this_thread();
    if (auto* strand = executor.target<boost::asio::strand<IoExecutor>>())
      return strand->get_inner_executor().running_in_this_thread();
    return false;
  }

  uint16_t port_;
  std::string bind_address_{"0.0.0.0"};
  std::string tls_certificate_file_;
  std::string tls_private_key_file_;
  std::shared_ptr<interface::Channel> channel_;
  std::shared_ptr<boost::asio::io_context> external_ioc_;
  std::atomic<bool> use_external_context_{false};
  std::atomic<bool> manage_external_context_{false};
  std::jthread external_thread_;
  std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> work_guard_;

  std::vector<std::promise<bool>> pending_promises_;
  std::atomic<bool> started_{false};
  std::atomic<bool> is_listening_{false};
  std::shared_ptr<bool> alive_marker_{std::make_shared<bool>(true)};

  // Configuration
  std::atomic<bool> auto_start_{false};
  std::atomic<bool> shared_context_{false};
  std::atomic<bool> port_retry_enabled_{false};
  std::atomic<int> max_port_retries_{3};
  std::atomic<int> port_retry_interval_ms_{1000};
  std::atomic<int> idle_timeout_ms_{static_cast<int>(base::constants::DEFAULT_IDLE_TIMEOUT_MS)};
  std::atomic<bool> client_limit_enabled_{false};
  std::atomic<size_t> max_clients_{0};
  std::atomic<size_t> backpressure_threshold_{base::constants::DEFAULT_BACKPRESSURE_THRESHOLD};
  std::atomic<base::constants::BackpressureStrategy> backpressure_strategy_{
      base::constants::BackpressureStrategy::Reliable};
  std::atomic<bool> tcp_no_delay_{base::constants::DEFAULT_TCP_NO_DELAY};
  std::atomic<bool> keep_alive_{base::constants::DEFAULT_KEEP_ALIVE};
  std::atomic<size_t> send_buffer_size_{0};
  std::atomic<size_t> receive_buffer_size_{0};
  std::atomic<size_t> read_buffer_size_{base::constants::DEFAULT_READ_BUFFER_SIZE};

  ConnectionHandler on_client_connect_{nullptr};
  ConnectionHandler on_disconnect_{nullptr};
  // Shared snapshots: the io thread copies one out per received chunk, and a
  // std::function copy allocates whenever the user handler outgrows its
  // small-object buffer. See interface::SharedCallback.
  interface::SharedCallback<MessageHandler> on_data_;
  interface::SharedCallback<BatchMessageHandler> data_batch_handler_;
  ErrorHandler on_error_{nullptr};
  std::function<void(size_t)> on_backpressure_{nullptr};
  FramerFactory framer_factory_{nullptr};
  interface::SharedCallback<MessageHandler> on_message_;
  interface::SharedCallback<BatchMessageHandler> message_batch_handler_;

  // Batching logic
  bool terminal_error_reported_ = false;
  void require_stopped_config() const {
    if (started_.load() || stop_callers_.load() || (stop_requested_ && alive_marker_) || detail::in_data_callback())
      throw std::logic_error("configuration requires completed stop");
  }
  ReceiveLimits receive_limits_;
  std::shared_ptr<detail::ReceiveBudget> receive_budget_{std::make_shared<detail::ReceiveBudget>(receive_limits_)};
  std::unordered_map<ClientId, std::shared_ptr<detail::SessionBatch>> batches_;
  size_t max_batch_size_ = 100;
  std::chrono::milliseconds max_batch_latency_{1};

  std::unordered_map<ClientId, std::shared_ptr<framer::IFramer>> framers_;
  // Cached transport pointer — set once in start(), avoids repeated dynamic_cast.
  std::shared_ptr<transport::TcpServer> transport_cache_;

  explicit Impl(uint16_t port)
      : port_(port),
        started_(false),
        is_listening_(false),
        auto_start_(false),
        port_retry_enabled_(false),
        max_port_retries_(3),
        port_retry_interval_ms_(1000),
        idle_timeout_ms_(static_cast<int>(base::constants::DEFAULT_IDLE_TIMEOUT_MS)),
        client_limit_enabled_(false),
        max_clients_(0),
        backpressure_threshold_(base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
        backpressure_strategy_(base::constants::BackpressureStrategy::Reliable) {}

  Impl(uint16_t port, std::shared_ptr<boost::asio::io_context> external_ioc)
      : port_(port),
        external_ioc_(std::move(external_ioc)),
        use_external_context_(external_ioc_ != nullptr),
        manage_external_context_(false),
        started_(false),
        is_listening_(false),
        auto_start_(false),
        port_retry_enabled_(false),
        max_port_retries_(3),
        port_retry_interval_ms_(1000),
        idle_timeout_ms_(static_cast<int>(base::constants::DEFAULT_IDLE_TIMEOUT_MS)),
        client_limit_enabled_(false),
        max_clients_(0),
        backpressure_threshold_(base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
        backpressure_strategy_(base::constants::BackpressureStrategy::Reliable) {}

  explicit Impl(std::shared_ptr<interface::Channel> channel)
      : port_(0),
        channel_(std::move(channel)),
        started_(false),
        is_listening_(false),
        auto_start_(false),
        port_retry_enabled_(false),
        max_port_retries_(3),
        port_retry_interval_ms_(1000),
        idle_timeout_ms_(static_cast<int>(base::constants::DEFAULT_IDLE_TIMEOUT_MS)),
        client_limit_enabled_(false),
        max_clients_(0),
        backpressure_threshold_(base::constants::DEFAULT_BACKPRESSURE_THRESHOLD),
        backpressure_strategy_(base::constants::BackpressureStrategy::Reliable) {
    injected_channel_ = true;
    transport_cache_ = std::dynamic_pointer_cast<transport::TcpServer>(channel_);
    // #450: setup_internal_handlers() captures weak_from_this() - calling it
    // from inside this constructor would capture an empty weak_ptr, since
    // enable_shared_from_this isn't wired up until make_shared() finishes
    // constructing the object. Deferred to TcpServer's own constructor,
    // which runs after impl_ is a fully-formed shared_ptr<Impl>.
  }

  ~Impl() {
    try {
      stop();
    } catch (...) {
    }
  }

  void fulfill_all_locked(bool value) {
    for (auto& p : pending_promises_) {
      try {
        p.set_value(value);
      } catch (...) {
      }
    }
    pending_promises_.clear();
  }

  void flush_batches(uint64_t generation, ClientId id, const std::shared_ptr<detail::SessionBatch>& state) {
    auto lease = callback_gate_.enter(generation);
    if (!lease.admitted()) return;
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto found = batches_.find(id);
    if (found == batches_.end() || found->second != state) return;
    state->scheduled = false;
    auto data = std::move(state->data);
    auto messages = std::move(state->messages);
    state->data.clear();
    state->messages.clear();
    auto data_handler = data_batch_handler_;
    auto message_handler = message_batch_handler_;
    lock.unlock();
    if (!data.empty()) detail::invoke_user_callback("tcp_server", "on_data_batch", data_handler, data);
    // A callback may stop the server. Recheck admission before another callback.
    auto next = callback_gate_.enter(generation);
    if (next.admitted() && !messages.empty())
      detail::invoke_user_callback("tcp_server", "on_message_batch", message_handler, messages);
  }

  // Caller holds mutex_; do not postpone an existing deadline when the
  // other queue receives its first item.
  void schedule_batch_timer(uint64_t generation, ClientId id) {
    auto found = batches_.find(id);
    if (found == batches_.end() || found->second->scheduled) return;
    auto state = found->second;
    state->scheduled = true;
    state->timer.expires_after(max_batch_latency_);
    state->timer.async_wait(
        [weak_impl = weak_from_this(), generation, id,
         weak_state = std::weak_ptr<detail::SessionBatch>(state)](const boost::system::error_code& ec) {
          if (ec) return;
          auto self = weak_impl.lock();
          auto batch = weak_state.lock();
          if (self && batch) self->flush_batches(generation, id, batch);
        });
  }

  std::future<bool> start() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    stop_requested_ = false;
    if (is_listening_.load()) {
      started_.store(true);
      std::promise<bool> p;
      p.set_value(true);
      return p.get_future();
    }
    std::promise<bool> p;
    auto f = p.get_future();
    pending_promises_.push_back(std::move(p));
    if (started_.exchange(true)) return f;

    if (!alive_marker_) alive_marker_ = std::make_shared<bool>(true);
    if (!channel_) {
      config::TcpServerConfig config;
      config.bind_address = bind_address_;
      config.port = port_;
      config.enable_port_retry = port_retry_enabled_.load();
      config.max_port_retries = max_port_retries_.load();
      config.port_retry_interval_ms = port_retry_interval_ms_.load();
      config.idle_timeout_ms = idle_timeout_ms_.load();
      config.backpressure_threshold = backpressure_threshold_.load();
      config.backpressure_strategy = backpressure_strategy_.load();
      config.tcp_no_delay = tcp_no_delay_.load();
      config.keep_alive = keep_alive_.load();
      config.send_buffer_size = send_buffer_size_.load();
      config.receive_buffer_size = receive_buffer_size_.load();
      config.read_buffer_size = read_buffer_size_.load();
      config.use_shared_context = shared_context_.load();
      config.tls_certificate_file = tls_certificate_file_;
      config.tls_private_key_file = tls_private_key_file_;

      channel_ = factory::ChannelFactory::create(config, external_ioc_);
      transport_cache_ = std::dynamic_pointer_cast<transport::TcpServer>(channel_);

      if (client_limit_enabled_.load()) {
        auto transport_server = std::dynamic_pointer_cast<transport::TcpServer>(channel_);
        if (transport_server) {
          transport_server->set_client_limit(max_clients_.load());
        }
      }
    }
    // D-1: registering the handlers is what opens this run's generation and
    // admits callbacks again - one step under the gate's lock, so a callback
    // of the previous run can never be admitted into this one. It runs on
    // every start, including on an injected channel whose object serves every
    // run.
    if (channel_) {
      transport_cache_ = std::dynamic_pointer_cast<transport::TcpServer>(channel_);
      setup_internal_handlers();
    }

    // #506: take a local copy of the shared_ptr before unlocking. Calling
    // through the raw channel_ member here would race a concurrent stop()'s
    // channel_.reset() on the member itself (not just the pointee) - TSAN
    // caught exactly this under TcpServerWrapperLifecycleTest.
    // ConcurrentStartStop. A local copy holds its own reference, so it stays
    // valid and race-free even if another thread resets the member.
    auto channel_copy = channel_;
    lock.unlock();
    channel_copy->start();
    if (use_external_context_.load() && manage_external_context_.load() && !external_thread_.joinable()) {
      if (external_ioc_ && external_ioc_->stopped()) {
        external_ioc_->restart();
      }
      work_guard_ = std::make_unique<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
          boost::asio::make_work_guard(*external_ioc_));
      external_thread_ = std::jthread([ioc = external_ioc_](std::stop_token st) {
        wirestead::concurrency::run_io_thread_init();
        try {
          std::stop_callback cb(st, [ioc] { ioc->stop(); });
          ioc->run();
        } catch (...) {
        }
      });
    }
    return f;
  }

  void stop() {
    stop_callers_.fetch_add(1);
    struct StopCall {
      std::atomic<unsigned>& count;
      ~StopCall() { count.fetch_sub(1); }
    } stop_call{stop_callers_};
    const bool request_only = shutdown_needs_this_thread();
    callback_gate_.close();
    std::shared_ptr<interface::Channel> channel;
    {
      std::unique_lock<std::shared_mutex> lock(mutex_);
      stop_requested_ = true;
      if (auto ts = transport_cache_) ts->cancel_target_waits();
      started_.store(false);
      is_listening_.store(false);
      bp_cv_.notify_all();
      fulfill_all_locked(false);
      channel = channel_;
    }
    // Do not hold wrapper locks while the transport waits for callbacks.
    if (channel) channel->stop();
    if (request_only) return;

    callback_gate_.wait_until_idle();
    // All outside callers pass this lock, including those that found the
    // channel already cleared. Joining and final state reset happen once.
    std::lock_guard<std::mutex> finalize_lock(stop_finalize_mutex_);
    {
      std::unique_lock<std::shared_mutex> lock(mutex_);
      alive_marker_.reset();
      batches_.clear();
      if (channel_) {
        channel_->on_bytes(nullptr);
        channel_->on_state(nullptr);
        channel_->on_backpressure(nullptr);
      }
      if (!injected_channel_) channel_.reset();
      transport_cache_.reset();
      framers_.clear();
    }
    if (use_external_context_.load() && manage_external_context_.load()) {
      if (work_guard_) work_guard_.reset();
      if (external_ioc_) external_ioc_->stop();
      if (external_thread_.joinable()) external_thread_.join();
    }
  }

  // Caller holds mutex_. Final admission rechecks the native lifecycle.
  SendResult send_state(const std::shared_ptr<transport::TcpServer>& ts) {
    if (stop_callers_.load() != 0) return SendResult::reject(SendRejection::Stopping);
    if (!started_.load()) {
      if (stop_requested_) {
        if (!callback_gate_.idle()) return SendResult::reject(SendRejection::Stopping);
        if (ts) {
          const auto state = ts->target_state();
          if (!state.accepted() && state.reason() == SendRejection::Stopping) return state;
        }
      }
      return SendResult::reject(SendRejection::NotStarted);
    }
    if (!ts) return SendResult::reject(SendRejection::NotReady);
    return SendResult::accept();
  }

  SendResult try_send_to(ClientId client_id, std::string_view data, bool best_effort_send = false) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    const auto& ts = transport_cache_;
    const auto result = [&]() -> SendResult {
      auto validation =
          detail::validate_payload_size(data.size(), ts ? ts->write_queue_limit(client_id) : std::nullopt);
      if (!validation.accepted()) return validation;
      const auto state = send_state(ts);
      if (!state.accepted()) return state;
      auto admitted = ts->write_target(
          client_id, memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(data.data()), data.size()), true);
      if (best_effort_send && !admitted.accepted() && admitted.reason() == SendRejection::WouldBlock)
        return SendResult::reject(SendRejection::QueueFull);
      return admitted;
    }();
    lock.unlock();
    if (auto hook = detail::g_tcp_server_send_result_hook.load()) hook(result);
    return result;
  }

  FanoutResult try_broadcast(std::string_view data, bool append_newline = false) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    const auto& ts = transport_cache_;
    return ts ? ts->broadcast_result({reinterpret_cast<const uint8_t*>(data.data()), data.size()}, send_state(ts),
                                     append_newline)
              : FanoutResult{};
  }

  SendResult send_to(ClientId client_id, std::string_view data) {
    if (backpressure_strategy_.load() == base::constants::BackpressureStrategy::Reliable)
      return send_to_blocking(client_id, data);
    return try_send_to(client_id, data, true);
  }

  FanoutResult broadcast(std::string_view data) {
    // In order to avoid Head-Of-Line blocking where a single slow client
    // blocks the entire broadcast loop, we delegate to try_broadcast (async fan-out)
    // even in Reliable mode. Transport-level backpressure will still protect the queues.
    return try_broadcast(data);
  }

  // channel_->on_backpressure()/session-level backpressure callbacks call bp_cv_.notify_all()
  // from the transport's io_context thread without holding bp_mutex_ - a classic lost-wakeup
  // race is possible: a waiter can check the predicate, find it still blocking, and be in the
  // process of registering to wait when the notify fires. Poll with a bounded timeout instead
  // of an unbounded wait() so a missed notify only costs a short delay rather than a
  // permanent hang (see #427, #431).
  //
  // Returns false without sending instead of waiting if called from the
  // channel's own io thread while backpressure is active for this client -
  // e.g. a blocking send_to() called from inside an on_data/on_message
  // callback. Clearing backpressure requires that same io thread to make
  // progress, so blocking here would deadlock forever rather than
  // eventually clear (#449).
  // #509: the wait predicate (is_backpressure_active(client_id), tied to
  // bp_high) and the transport's own hard queue-byte cap for that session
  // (bp_limit) are different thresholds observed at different times, so a
  // single write attempt can spuriously fail right after the wait exits.
  // Retry capacity races until admission or cancellation. Oversized payloads
  // are rejected by validation before entering this loop.

  SendResult send_to_blocking(ClientId client_id, std::string_view data) {
    std::shared_ptr<transport::TcpServer> ts;
    std::shared_ptr<transport::TcpServerSession> session;
    uint64_t generation = 0;
    bool cannot_wait = false;
    const auto result = [&]() -> SendResult {
      {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        ts = transport_cache_;
        auto validation =
            detail::validate_payload_size(data.size(), ts ? ts->write_queue_limit(client_id) : std::nullopt);
        if (!validation.accepted()) return validation;
        auto state = send_state(ts);
        if (!state.accepted()) return state;
        state = ts->target_state();
        if (!state.accepted()) return state;
        generation = callback_generation_.load();
        cannot_wait = detail::in_data_callback() || detail::executor_running_here(ts->get_executor());
        session = ts->capture_target(client_id);
        if (!session) return SendResult::reject(SendRejection::NotReady);
      }
      auto wait = [&]() -> SendResult {
        std::unique_lock<std::mutex> bp_lock(bp_mutex_);
        {
          std::shared_lock<std::shared_mutex> lock(mutex_);
          // No observed pressure means no capacity wait. Final admission still
          // checks lifecycle and the exact session selected at entry.
          if (!started_.load() || callback_generation_.load() != generation ||
              ts->poll_target_wait(session).has_value())
            return SendResult::accept();
        }
        if (cannot_wait) return SendResult::reject(SendRejection::WouldBlock);
        if (auto hook = detail::g_tcp_server_capacity_wait_hook.load()) hook();
        std::optional<SendResult> outcome;
        auto released = [&] {
          // The retained session owns its first terminal cause, even after
          // removal from the map or a later server/wrapper restart.
          outcome = ts->poll_target_wait(session);
          return outcome.has_value();
        };
        while (!bp_cv_.wait_for(bp_lock, std::chrono::milliseconds(50), released)) {
        }
        if (auto hook = detail::g_tcp_server_capacity_wait_result_hook.load()) hook(*outcome);
        return *outcome;
      };
      for (bool retry = false;; retry = true) {
        if (retry) detail::pause_send_retry(bp_cv_, bp_mutex_);
        const auto released = wait();
        if (!released.accepted()) return released;
        std::shared_lock<std::shared_mutex> lock(mutex_);
        const auto state = send_state(ts);
        if (!state.accepted()) return state;
        if (callback_generation_.load() != generation) return SendResult::reject(SendRejection::NotReady);
        const auto admitted = ts->write_target(
            client_id, memory::ConstByteSpan(reinterpret_cast<const uint8_t*>(data.data()), data.size()), false,
            session);
        if (admitted.accepted() || admitted.reason() != SendRejection::WouldBlock) return admitted;
        if (cannot_wait) return admitted;
      }
    }();
    if (auto hook = detail::g_tcp_server_send_result_hook.load()) hook(result);
    return result;
  }

  void setup_internal_handlers() {
    if (!channel_) return;

    std::weak_ptr<bool> weak_alive = alive_marker_;
    std::weak_ptr<Impl> weak_impl = weak_from_this();
    // Opening this run's generation and admitting again are one step under
    // the gate's lock.
    receive_budget_->reset_stats();
    terminal_error_reported_ = false;
    const uint64_t generation = callback_gate_.open_new_generation();
    callback_generation_.store(generation);
    auto transport_server = std::dynamic_pointer_cast<transport::TcpServer>(channel_);
    if (transport_server) {
      transport_server->on_multi_connect([this, generation, weak_impl, weak_alive,
                                          weak_native = std::weak_ptr<transport::TcpServer>(transport_server)](
                                             ClientId id, const std::string& info) {
        auto impl_keepalive = weak_impl.lock();
        if (!impl_keepalive) return;
        auto alive = weak_alive.lock();
        if (!alive) return;
        auto lease = callback_gate_.enter(generation);
        if (!lease.admitted()) return;

        auto native = weak_native.lock();
        auto executor = native ? native->client_executor(id) : std::nullopt;
        if (!executor) return;
        auto receive_scope = receive_budget_->open_scope();
        if (!receive_scope) {
          native->fail_receive(id);
          return;
        }

        ConnectionHandler handler;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          batches_[id] = std::make_shared<detail::SessionBatch>(*executor, std::move(receive_scope));
          if (framer_factory_) {
            auto framer = framer_factory_();
            if (framer) {
              auto shared_framer = std::shared_ptr<framer::IFramer>(std::move(framer));
              shared_framer->on_message([this, id, generation](memory::ConstByteSpan msg) {
                auto message_lease = callback_gate_.enter(generation);
                if (!message_lease.admitted()) return;
                auto prepared = detail::take_prepared_message(id, msg);
                // #441: snapshot under a shared_lock (pure read), build the
                // copy before taking the exclusive lock for queue mutation.
                bool batch_mode;
                interface::SharedCallback<MessageHandler> on_message_handler;
                {
                  std::shared_lock<std::shared_mutex> lock(mutex_);
                  batch_mode = static_cast<bool>(message_batch_handler_);
                  on_message_handler = on_message_;
                }

                if (batch_mode) {
                  std::shared_ptr<detail::SessionBatch> receive;
                  {
                    std::shared_lock<std::shared_mutex> lock(mutex_);
                    auto state = batches_.find(id);
                    if (state == batches_.end()) return;
                    receive = state->second;
                  }
                  auto ctx = prepared ? std::move(*prepared) : detail::retain_received(receive->receive.scope, id, msg);
                  interface::SharedCallback<BatchMessageHandler> flush_handler;
                  detail::ReceiveBatch batch;
                  {
                    std::unique_lock<std::shared_mutex> lock(mutex_);
                    auto state = batches_.find(id);
                    if (state == batches_.end() || callback_generation_ != generation) return;
                    state->second->messages.emplace_back(std::move(ctx));
                    if (batches_.at(id)->messages.size() >= max_batch_size_) {
                      flush_handler = message_batch_handler_;
                      batch = std::move(batches_.at(id)->messages);
                      batches_.at(id)->messages.clear();
                    } else if (batches_.at(id)->messages.size() == 1) {
                      schedule_batch_timer(generation, id);
                    }
                  }
                  detail::invoke_user_callback("tcp_server", "on_message_batch", flush_handler, batch);
                  return;
                }

                detail::invoke_user_callback("tcp_server", "on_message", on_message_handler, MessageContext(id, msg));
              });
              framers_[id] = std::move(shared_framer);
            }
          }
          handler = on_client_connect_;
        }
        detail::invoke_user_callback("tcp_server", "on_connect", handler, ConnectionContext(id, info));
      });
      transport_server->on_multi_data(
          [this, generation, weak_impl, weak_alive](ClientId id, memory::ConstByteSpan data_span) {
            auto impl_keepalive = weak_impl.lock();
            if (!impl_keepalive) return;
            auto alive = weak_alive.lock();
            if (!alive) return;

            // #449: everything below runs synchronously on this io thread -
            // mark it so a blocking send_to()/broadcast() called from within
            // one of these callbacks fails fast instead of deadlocking.
            auto lease = callback_gate_.enter(generation);
            if (!lease.admitted()) return;
            detail::CallbackGuard callback_guard;

            // #441: snapshot the handler/framer pointers under a shared_lock
            // (not unique_lock) - this is a pure read, matching try_send's
            // locking level so it no longer blocks concurrent sends even
            // briefly.
            bool batch_mode;
            std::shared_ptr<detail::SessionBatch> receive;
            interface::SharedCallback<MessageHandler> handler;
            std::shared_ptr<framer::IFramer> framer;
            {
              std::shared_lock<std::shared_mutex> lock(mutex_);
              auto state = batches_.find(id);
              if (state == batches_.end()) return;
              receive = state->second;
              batch_mode = static_cast<bool>(data_batch_handler_);
              handler = on_data_;
              auto it = framers_.find(id);
              if (it != framers_.end()) {
                framer = it->second;
              }
            }

            try {
              auto prepared = detail::prepare_receive(receive->receive, framer, id, data_span, batch_mode);
              if (batch_mode) {
                // #441: build the copy before taking the exclusive lock, so the
                // lock is only held for the queue mutation itself, not the
                // allocation.
                auto ctx = std::move(*prepared.raw);
                interface::SharedCallback<BatchMessageHandler> flush_handler;
                detail::ReceiveBatch batch;
                {
                  std::unique_lock<std::shared_mutex> lock(mutex_);
                  auto state = batches_.find(id);
                  if (state == batches_.end() || callback_generation_ != generation) return;
                  state->second->data.emplace_back(std::move(ctx));
                  if (batches_.at(id)->data.size() >= max_batch_size_) {
                    flush_handler = data_batch_handler_;
                    batch = std::move(batches_.at(id)->data);
                    batches_.at(id)->data.clear();
                  } else if (batches_.at(id)->data.size() == 1) {
                    schedule_batch_timer(generation, id);
                  }
                }
                detail::invoke_user_callback("tcp_server", "on_data_batch", flush_handler, batch);
              } else {
                detail::invoke_user_callback("tcp_server", "on_data", handler, MessageContext(id, data_span));
              }

              prepared.deliver();
            } catch (const detail::ReceiveOverflow& overflow) {
              receive->receive.scope->overflow(data_span.size(), overflow.reason);

              {
                std::unique_lock<std::shared_mutex> lock(mutex_);
                receive->data.clear();
                receive->messages.clear();
                receive->receive.reset(framer.get());
              }
              if (auto native = std::dynamic_pointer_cast<transport::TcpServer>(channel_)) native->fail_receive(id);

            } catch (const std::bad_alloc&) {
              receive->receive.scope->overflow(data_span.size(), ReceiveOverflowReason::AllocationFailure);

              {
                std::unique_lock<std::shared_mutex> lock(mutex_);
                receive->data.clear();
                receive->messages.clear();
                receive->receive.reset(framer.get());
              }
              if (auto native = std::dynamic_pointer_cast<transport::TcpServer>(channel_)) native->fail_receive(id);
            }
          });
      transport_server->on_multi_disconnect([this, generation, weak_impl, weak_alive](ClientId id) {
        auto impl_keepalive = weak_impl.lock();
        if (!impl_keepalive) return;
        auto alive = weak_alive.lock();
        if (!alive) return;
        auto lease = callback_gate_.enter(generation);
        if (!lease.admitted()) return;

        std::shared_ptr<detail::SessionBatch> pending;
        {
          std::shared_lock<std::shared_mutex> lock(mutex_);
          auto it = batches_.find(id);
          if (it != batches_.end()) pending = it->second;
        }
        if (!pending) return;
        flush_batches(generation, id, pending);
        auto after_flush = callback_gate_.enter(generation);
        if (!after_flush.admitted()) return;
        ConnectionHandler handler;
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          batches_.erase(id);
          framers_.erase(id);
          handler = on_disconnect_;
        }
        detail::invoke_user_callback("tcp_server", "on_disconnect", handler, ConnectionContext(id));
      });

      transport_server->on_backpressure([this, generation, weak_impl, weak_alive](size_t queued) {
        bp_cv_.notify_all();
        auto impl_keepalive = weak_impl.lock();
        if (!impl_keepalive) return;
        auto alive = weak_alive.lock();
        if (!alive) return;
        auto lease = callback_gate_.enter(generation);
        if (!lease.admitted()) return;
        std::function<void(size_t)> handler;
        {
          std::shared_lock<std::shared_mutex> lock(mutex_);
          handler = on_backpressure_;
        }
        detail::invoke_user_callback("tcp_server", "on_backpressure", handler, queued);
      });
    }
    channel_->on_state([this, generation, weak_impl, weak_alive](base::LinkState state) {
      auto impl_keepalive = weak_impl.lock();
      if (!impl_keepalive) return;
      auto alive = weak_alive.lock();
      if (!alive) return;
      auto lease = callback_gate_.enter(generation);
      if (!lease.admitted()) return;

      if (state == base::LinkState::Listening) {
        is_listening_.store(true);
        std::unique_lock<std::shared_mutex> lock(mutex_);
        terminal_error_reported_ = false;
        fulfill_all_locked(true);
      } else if (state == base::LinkState::Error || state == base::LinkState::Closed ||
                 state == base::LinkState::Idle) {
        ErrorHandler handler;
        is_listening_.store(false);
        {
          std::unique_lock<std::shared_mutex> lock(mutex_);
          fulfill_all_locked(false);
          if (state == base::LinkState::Error && !terminal_error_reported_) {
            terminal_error_reported_ = true;
            handler = on_error_;
          }
        }
        detail::invoke_user_callback("tcp_server", "on_error", handler,
                                     channel_ ? detail::build_error_context(*channel_, "Server error")
                                              : ErrorContext(ErrorCode::IoError, "Server error"));
      }
    });
  }

  RuntimeStats stats() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return channel_ ? channel_->stats() : RuntimeStats{};
  }

  void reset_stats() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    receive_budget_->reset_stats();
    if (channel_) channel_->reset_stats();
  }
};

TcpServer::TcpServer(uint16_t port) : impl_(std::make_shared<Impl>(port)) {}
TcpServer::TcpServer(uint16_t port, std::shared_ptr<boost::asio::io_context> ioc)
    : impl_(std::make_shared<Impl>(port, ioc)) {}
TcpServer::TcpServer(std::shared_ptr<interface::Channel> ch) : impl_(std::make_shared<Impl>(ch)) {
  impl_->setup_internal_handlers();
}
TcpServer::~TcpServer() = default;

TcpServer::TcpServer(TcpServer&&) noexcept = default;
TcpServer& TcpServer::operator=(TcpServer&&) noexcept = default;

TcpServer& TcpServer::receive_limits(ReceiveLimits limits) {
  limits.validate();
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  if (impl_->started_ || impl_->stop_callers_.load() || (impl_->stop_requested_ && impl_->alive_marker_) ||
      detail::in_data_callback())
    throw std::logic_error("receive limits require completed stop");
  auto budget = std::make_shared<detail::ReceiveBudget>(limits);
  impl_->receive_limits_ = limits;
  impl_->receive_budget_ = std::move(budget);
  return *this;
}
ReceiveMemoryStats TcpServer::receive_stats() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  return impl_->receive_budget_->stats();
}

std::future<bool> TcpServer::start() { return impl_->start(); }
void TcpServer::stop() { impl_->stop(); }
bool TcpServer::listening() const { return get_impl()->is_listening_.load(); }
RuntimeStats TcpServer::stats() const { return get_impl()->stats(); }
void TcpServer::reset_stats() { impl_->reset_stats(); }

FanoutResult TcpServer::broadcast(std::string_view data) { return impl_->broadcast(data); }
FanoutResult TcpServer::try_broadcast(std::string_view data) { return impl_->try_broadcast(data); }
SendResult TcpServer::send_to(ClientId client_id, std::string_view data) { return impl_->send_to(client_id, data); }
SendResult TcpServer::try_send_to(ClientId client_id, std::string_view data) {
  return impl_->try_send_to(client_id, data);
}

SendResult TcpServer::send_to_blocking(ClientId client_id, std::string_view data) {
  return impl_->send_to_blocking(client_id, data);
}

FanoutResult TcpServer::broadcast_line(std::string_view line) { return impl_->try_broadcast(line, true); }
SendResult TcpServer::send_to_line(ClientId client_id, std::string_view line) {
  return send_to(client_id, std::string(line) + "\n");
}
FanoutResult TcpServer::try_broadcast_line(std::string_view line) { return impl_->try_broadcast(line, true); }
SendResult TcpServer::try_send_to_line(ClientId client_id, std::string_view line) {
  return try_send_to(client_id, std::string(line) + "\n");
}

TcpServer& TcpServer::on_connect(ConnectionHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_client_connect_ = std::move(h);
  return *this;
}
TcpServer& TcpServer::on_disconnect(ConnectionHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_disconnect_ = std::move(h);
  return *this;
}
TcpServer& TcpServer::on_data(MessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_data_ = interface::share_callback(std::move(h));
  return *this;
}
TcpServer& TcpServer::on_data_batch(BatchMessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->data_batch_handler_ = interface::share_callback(std::move(h));
  return *this;
}
TcpServer& TcpServer::on_error(ErrorHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_error_ = std::move(h);
  return *this;
}

TcpServer& TcpServer::on_backpressure(std::function<void(size_t)> h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_backpressure_ = std::move(h);
  return *this;
}

TcpServer& TcpServer::framer(FramerFactory factory) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->framer_factory_ = std::move(factory);
  return *this;
}

TcpServer& TcpServer::on_message(MessageHandler handler) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->on_message_ = interface::share_callback(std::move(handler));
  return *this;
}

TcpServer& TcpServer::on_message_batch(BatchMessageHandler h) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->message_batch_handler_ = interface::share_callback(std::move(h));
  return *this;
}

size_t TcpServer::client_count() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  const auto& ts = get_impl()->transport_cache_;
  return ts ? ts->client_count() : 0;
}

std::vector<ClientId> TcpServer::connected_clients() const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  const auto& ts = get_impl()->transport_cache_;
  return ts ? ts->connected_clients() : std::vector<ClientId>();
}

std::optional<RuntimeStats> TcpServer::client_stats(ClientId client_id) const {
  std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
  const auto& ts = get_impl()->transport_cache_;
  return ts ? ts->client_stats(client_id) : std::nullopt;
}

TcpServer& TcpServer::auto_start(bool m) {
  impl_->auto_start_.store(m);
  if (impl_->auto_start_.load() && !impl_->started_.load()) start();
  return *this;
}

TcpServer& TcpServer::bind_address(const std::string& address) {
  config::detail::require(util::InputValidator::is_valid_ipv4(address) || util::InputValidator::is_valid_ipv6(address),
                          "invalid bind address");
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->require_stopped_config();
  impl_->bind_address_ = address;
  return *this;
}

TcpServer& TcpServer::shared_context(bool use_shared) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->require_stopped_config();
  impl_->shared_context_.store(use_shared);
  return *this;
}

TcpServer& TcpServer::port_retry(bool e, int m, int i) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->require_stopped_config();
  config::detail::range(m, 0, base::constants::MAX_RETRIES_LIMIT, "invalid port retry count");
  config::detail::range(i, base::constants::MIN_RETRY_INTERVAL_MS, base::constants::MAX_RETRY_INTERVAL_MS,
                        "invalid port retry interval");
  impl_->port_retry_enabled_.store(e);
  impl_->max_port_retries_.store(m);
  impl_->port_retry_interval_ms_.store(i);
  return *this;
}

TcpServer& TcpServer::idle_timeout(std::chrono::milliseconds timeout) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->require_stopped_config();
  config::detail::duration(timeout, base::constants::MIN_IDLE_TIMEOUT_MS, base::constants::MAX_IDLE_TIMEOUT_MS, true,
                           "invalid idle_timeout");
  impl_->idle_timeout_ms_.store(static_cast<int>(timeout.count()));
  return *this;
}

TcpServer& TcpServer::max_clients(size_t max) {
  config::detail::range(max, 0, base::constants::MAX_MAX_CONNECTIONS, "invalid client limit");
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_clients_.store(max);
  if (max == 0) {
    impl_->client_limit_enabled_.store(false);
  } else {
    impl_->client_limit_enabled_.store(true);
  }
  if (impl_->transport_cache_) impl_->transport_cache_->set_client_limit(max);
  return *this;
}

TcpServer& TcpServer::backpressure_threshold(size_t threshold) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->require_stopped_config();
  config::detail::range(threshold, base::constants::MIN_BACKPRESSURE_THRESHOLD,
                        base::constants::MAX_BACKPRESSURE_THRESHOLD, "invalid backpressure_threshold");
  impl_->backpressure_threshold_.store(threshold);
  return *this;
}

TcpServer& TcpServer::backpressure_strategy(base::constants::BackpressureStrategy strategy) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->require_stopped_config();
  config::detail::strategy(strategy);
  impl_->backpressure_strategy_.store(strategy);
  return *this;
}

size_t TcpServer::backpressure_threshold() const { return impl_->backpressure_threshold_.load(); }

base::constants::BackpressureStrategy TcpServer::backpressure_strategy() const {
  return impl_->backpressure_strategy_.load();
}

TcpServer& TcpServer::tcp_no_delay(bool enable) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->require_stopped_config();
  impl_->tcp_no_delay_.store(enable);
  return *this;
}

TcpServer& TcpServer::keep_alive(bool enable) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->require_stopped_config();
  impl_->keep_alive_.store(enable);
  return *this;
}

TcpServer& TcpServer::tls(const std::string& certificate_file, const std::string& private_key_file) {
  config::detail::require(certificate_file.empty() == private_key_file.empty(), "TLS requires certificate and key");
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->require_stopped_config();
  impl_->tls_certificate_file_ = certificate_file;
  impl_->tls_private_key_file_ = private_key_file;
  return *this;
}

TcpServer& TcpServer::send_buffer_size(size_t bytes) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->require_stopped_config();
  if (bytes != 0)
    config::detail::range(bytes, base::constants::MIN_SOCKET_BUFFER_SIZE, base::constants::MAX_SOCKET_BUFFER_SIZE,
                          "invalid send_buffer_size");
  impl_->send_buffer_size_.store(bytes);
  return *this;
}

TcpServer& TcpServer::receive_buffer_size(size_t bytes) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->require_stopped_config();
  if (bytes != 0)
    config::detail::range(bytes, base::constants::MIN_SOCKET_BUFFER_SIZE, base::constants::MAX_SOCKET_BUFFER_SIZE,
                          "invalid receive_buffer_size");
  impl_->receive_buffer_size_.store(bytes);
  return *this;
}

TcpServer& TcpServer::read_buffer_size(size_t bytes) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->require_stopped_config();
  config::detail::range(bytes, base::constants::MIN_READ_BUFFER_SIZE, base::constants::MAX_READ_BUFFER_SIZE,
                        "invalid read_buffer_size");
  impl_->read_buffer_size_.store(bytes);
  return *this;
}

TcpServer& TcpServer::manage_external_context(bool m) {
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->require_stopped_config();
  impl_->manage_external_context_.store(m);
  return *this;
}

TcpServer& TcpServer::batch_size(size_t size) {
  config::detail::require(size > 0, "batch size must be positive");
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_batch_size_ = size;
  return *this;
}

TcpServer& TcpServer::batch_latency(std::chrono::milliseconds latency) {
  config::detail::duration(latency, 0, std::numeric_limits<int>::max(), true, "invalid batch latency");
  std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
  impl_->max_batch_latency_ = latency;
  return *this;
}

}  // namespace wrapper
}  // namespace wirestead

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

#include <algorithm>
#include <functional>
#include <limits>
#include <optional>
#include <typeinfo>

#include "wirestead/framer/length_prefix_framer.hpp"
#include "wirestead/framer/line_framer.hpp"
#include "wirestead/framer/packet_framer.hpp"
#include "wirestead/wrapper/context.hpp"
#include "wirestead/wrapper/receive_budget.hpp"

namespace wirestead::wrapper::detail {
struct ReceiveOverflow {
  ReceiveOverflowReason reason;
};
inline std::shared_ptr<ReceiveCharge> reserve_receive(const std::shared_ptr<ReceiveScope>& scope, size_t bytes) {
  auto charge = scope->reserve(bytes);
  if (!charge) throw ReceiveOverflow{ReceiveOverflowReason::ByteLimit};
  return charge;
}

// Reservation precedes payload allocation and outlives the payload, including
// a batch moved out of the queue into a user callback.
struct ReceiveMessage {
  std::shared_ptr<ReceiveCharge> charge;
  MessageContext context;
  ReceiveMessage(std::shared_ptr<ReceiveCharge> c, ClientId id, memory::ConstByteSpan bytes)
      : charge(std::move(c)), context(id, memory::SafeDataBuffer(bytes)) {}
  ReceiveMessage(ReceiveMessage&&) noexcept = default;
  ReceiveMessage& operator=(ReceiveMessage&&) noexcept = default;
};
inline thread_local ReceiveMessage* prepared_message = nullptr;
class PreparedMessageGuard {
 public:
  explicit PreparedMessageGuard(ReceiveMessage& message) : previous_(prepared_message) { prepared_message = &message; }
  ~PreparedMessageGuard() { prepared_message = previous_; }

 private:
  ReceiveMessage* previous_;
};
// Consume staged ownership at the wrapper message boundary, before any user
// callback can reenter another receive path. Raw input retention never adopts
// another dispatch's storage, even when it happens to borrow the same address.
inline std::optional<ReceiveMessage> take_prepared_message(ClientId id, memory::ConstByteSpan bytes) {
  if (prepared_message && prepared_message->charge && prepared_message->context.client_id() == id &&
      prepared_message->context.safe_data().data() == bytes.data() &&
      prepared_message->context.safe_data().size() == bytes.size()) {
    return std::move(*prepared_message);
  }
  return std::nullopt;
}
inline ReceiveMessage retain_received(const std::shared_ptr<ReceiveScope>& scope, ClientId id,
                                      memory::ConstByteSpan bytes) {
  // Conservative record allowance also covers staging and vector growth.
  constexpr size_t records = 4 * (sizeof(MessageContext) + sizeof(std::shared_ptr<ReceiveCharge>));
  if (bytes.size() > std::numeric_limits<size_t>::max() - records)
    throw ReceiveOverflow{ReceiveOverflowReason::ByteLimit};
  return ReceiveMessage(reserve_receive(scope, bytes.size() + records), id, bytes);
}

class ReceiveBatch {
 public:
  ReceiveBatch() = default;
  ReceiveBatch(ReceiveBatch&&) noexcept = default;
  ReceiveBatch& operator=(ReceiveBatch&& other) noexcept {
    if (this != &other) {
      clear();
      charges_ = std::move(other.charges_);
      messages_ = std::move(other.messages_);
    }
    return *this;
  }
  bool empty() const { return messages_.empty(); }
  size_t size() const { return messages_.size(); }
  void clear() {
    std::vector<MessageContext>().swap(messages_);
    std::vector<std::shared_ptr<ReceiveCharge>>().swap(charges_);
  }
  void emplace_back(ReceiveMessage message) {
    charges_.push_back(message.charge);
    try {
      messages_.push_back(std::move(message.context));
    } catch (...) {
      charges_.pop_back();
      throw;
    }
  }
  operator const std::vector<MessageContext>&() const { return messages_; }

 private:
  std::vector<std::shared_ptr<ReceiveCharge>> charges_;
  std::vector<MessageContext> messages_;
};

// Friend access changes neither existing public virtual tables nor layouts.
// Exact type checks intentionally exclude custom subclasses with hidden state.
struct ReceiveFramerAccess {
  template <class F>
  static F* exact(framer::IFramer* f) {
    return f && typeid(*f) == typeid(F) ? static_cast<F*>(f) : nullptr;
  }
  template <class Fn>
  static bool visit(framer::IFramer* f, Fn&& fn) {
    if (auto* p = exact<framer::LineFramer>(f)) {
      fn(*p);
      return true;
    }
    if (auto* p = exact<framer::LengthPrefixFramer>(f)) {
      fn(*p);
      return true;
    }
    if (auto* p = exact<framer::PacketFramer>(f)) {
      fn(*p);
      return true;
    }
    return false;
  }
  static size_t capacity(framer::IFramer* f) {
    size_t n = 0;
    visit(f, [&](auto& p) { n = p.buffer_.capacity(); });
    return n;
  }
  static size_t size(framer::IFramer* f) {
    size_t n = 0;
    visit(f, [&](auto& p) { n = p.buffer_.size(); });
    return n;
  }
  static bool supported(framer::IFramer* f) {
    return visit(f, [](auto&) {});
  }
  static std::unique_ptr<framer::IFramer> clone(framer::IFramer* f) {
    std::unique_ptr<framer::IFramer> result;
    visit(f, [&](auto& p) {
      using F = std::decay_t<decltype(p)>;
      result = std::make_unique<F>(p);
    });
    return result;
  }
  static void reserve_exact(framer::IFramer* f, size_t n) {
    visit(f, [&](auto& p) {
      if (n <= p.buffer_.capacity()) return;
      std::vector<uint8_t> replacement;
      replacement.reserve(n);
      replacement.insert(replacement.end(), p.buffer_.begin(), p.buffer_.end());
      p.buffer_.swap(replacement);
    });
  }
  static void commit(framer::IFramer* target, framer::IFramer* source) {
    visit(target, [&](auto& p) {
      using F = std::decay_t<decltype(p)>;
      auto& q = *static_cast<F*>(source);
      p.buffer_.swap(q.buffer_);
      if constexpr (std::is_same_v<F, framer::LineFramer>) {
        std::swap(p.scanned_idx_, q.scanned_idx_);
        std::swap(p.discarding_, q.discarding_);
      } else if constexpr (std::is_same_v<F, framer::PacketFramer>) {
        std::swap(p.scanned_idx_, q.scanned_idx_);
        std::swap(p.state_, q.state_);
      }
    });
  }
  static framer::IFramer::MessageCallback callback(framer::IFramer* f) {
    framer::IFramer::MessageCallback result;
    visit(f, [&](auto& p) { result = p.on_message_; });
    return result;
  }
  static void release_storage(framer::IFramer* f) {
    if (!f) return;
    f->reset();
    visit(f, [](auto& p) { std::vector<uint8_t>().swap(p.buffer_); });
  }
};
struct ReceiveState {
  explicit ReceiveState(std::shared_ptr<ReceiveScope> s) : scope(std::move(s)) {}
  std::shared_ptr<ReceiveScope> scope;
  std::shared_ptr<ReceiveCharge> frame_storage;
  void reset(framer::IFramer* f) {
    ReceiveFramerAccess::release_storage(f);
    frame_storage.reset();
  }
};

struct PreparedReceive {
  std::optional<ReceiveMessage> raw;
  std::vector<ReceiveMessage> messages;
  std::shared_ptr<framer::IFramer> framer;
  memory::ConstByteSpan input;
  bool builtin = false;
  void deliver() {
    if (!framer) return;
    if (!builtin) {
      framer->push_bytes(input);
      return;
    }
    auto callback = ReceiveFramerAccess::callback(framer.get());
    if (!callback) return;
    for (auto& message : messages) {
      auto bytes = message.context.safe_data().as_span();
      PreparedMessageGuard guard(message);
      callback(bytes);
    }
  }
};

// Stage a whole input before exposing callbacks. A rejected UDP datagram cannot
// partially alter an existing built-in framer or leak some decoded messages.
inline PreparedReceive prepare_receive(ReceiveState& state, const std::shared_ptr<framer::IFramer>& f, ClientId id,
                                       memory::ConstByteSpan input, bool raw_batch) {
  PreparedReceive result;
  result.framer = f;
  result.input = input;
  if (raw_batch) result.raw.emplace(retain_received(state.scope, id, input));
  if (!f || !ReceiveFramerAccess::supported(f.get())) return result;
  result.builtin = true;
  const auto limit = state.scope->limits().max_frame_bytes;
  const auto capacity = ReceiveFramerAccess::capacity(f.get());
  if (capacity > limit) throw ReceiveOverflow{ReceiveOverflowReason::FrameLimit};
  if (!state.frame_storage && capacity) state.frame_storage = reserve_receive(state.scope, capacity);
  auto work_charge = reserve_receive(state.scope, capacity);
  auto work = ReceiveFramerAccess::clone(f.get());
  work->on_message(
      [&](memory::ConstByteSpan message) { result.messages.emplace_back(retain_received(state.scope, id, message)); });
  size_t offset = 0;
  while (offset < input.size()) {
    const auto used = ReceiveFramerAccess::size(work.get());
    if (used >= limit) throw ReceiveOverflow{ReceiveOverflowReason::FrameLimit};
    const auto n = std::min({input.size() - offset, limit - used, size_t(4096)});
    const auto needed = used + n;
    if (needed > ReceiveFramerAccess::capacity(work.get())) {
      // Keep the old allocation charged until reserve_exact has destroyed it.
      auto next = reserve_receive(state.scope, needed);
      ReceiveFramerAccess::reserve_exact(work.get(), needed);
      work_charge = std::move(next);
    }
    work->push_bytes(input.subspan(offset, n));
    offset += n;
  }
  ReceiveFramerAccess::commit(f.get(), work.get());
  work.reset();  // destroy old source storage before releasing its reservation
  state.frame_storage = std::move(work_charge);
  return result;
}
}  // namespace wirestead::wrapper::detail

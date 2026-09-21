# Callback Data Lifetime

`MessageContext::data()` returns a callback-scoped view. The view is valid only
while the current callback is running.

Use an ownership-copy helper before sending data to another thread, queue,
coroutine, or longer-lived object.

```cpp
client->on_data([](const wirestead::MessageContext& ctx) {
    auto owned = ctx.data_as_string();
    post_to_worker([owned = std::move(owned)] {
        process(owned);
    });
});
```

Do not store `std::string_view` or spans returned from callback context objects.

```cpp
client->on_data([](const wirestead::MessageContext& ctx) {
    auto view = ctx.data();
    post_to_worker([view] {
        process(view);  // dangling risk
    });
});
```

For binary payloads, use `data_as_vector()` before the callback returns.

Copying the context object itself is also safe. A `MessageContext` delivered to
`on_data`/`on_message` borrows the transport's receive buffer so the callback
costs no allocation, but a copy always takes ownership of the payload, so a
copy stays valid after the callback returns:

```cpp
client->on_data([&](const wirestead::MessageContext& ctx) {
    queue.push_back(ctx);  // copy — owns its payload, safe to keep
});
```

Only the view returned by `data()` is callback-scoped. Contexts delivered to
the batch handlers (`on_data_batch`/`on_message_batch`) always own their data.

## Arrival time

`MessageContext::received_at()` is a `std::chrono::steady_clock::time_point`
stamped where the context was built, which on every receive path is the moment
the bytes came off the transport. Prefer it over reading a clock inside the
callback, which times the callback rather than the arrival:

- a batch handler receives one stamp per context, each from when that chunk
  arrived — reading the clock in the handler stamps the whole batch with the
  flush time instead;
- a framer that finds several messages in one read gives each of them that
  read's arrival time, rather than timing how far the parse got.

The clock is steady rather than system so the value survives an NTP step. To
put it on a wall clock, subtract the age instead of converting the epoch:

```cpp
client->on_message([&](const wirestead::MessageContext& ctx) {
    const auto age = std::chrono::steady_clock::now() - ctx.received_at();
    msg.header.stamp = node->now() - rclcpp::Duration(age);
});
```

This is transport arrival, not a kernel or hardware timestamp: it includes
whatever the kernel spent before the read completed.

## Choosing a framer

`on_message()` needs a framer, and which one is not a style choice:

| framer | splits on | use when |
| --- | --- | --- |
| `use_line_framer(delim)` | a delimiter, `\n` by default | text-line protocols |
| `use_packet_framer(start, end, max)` | a start and end byte pattern | the payload cannot contain the end pattern |
| `use_length_prefix_framer(bytes, endian, max)` | a length prefix, 2 bytes big-endian by default | binary payloads |

`use_packet_framer()` does no escaping. The **first** occurrence of the end
pattern ends the frame, wherever it falls, so a binary payload that happens to
contain those bytes is truncated — and the byte counters still balance, so
nothing anywhere reports an error. The reader sees a short message and has no
way to tell it from a real one.

`use_length_prefix_framer()` has no special byte values: the length says how
many bytes to collect, so the payload may hold anything.

```cpp
.use_length_prefix_framer(4)   // 4-byte big-endian length, excluding the prefix
.on_message([](const wirestead::MessageContext& ctx) { /* ... */ })
```

Its own limit is the mirror image: there is no sync word, so it cannot
resynchronise and needs a stream that starts on a frame boundary. That is the
normal case for TCP and UDS, and wrong for a serial line joined mid-stream —
there, a framer with a sync word is what you want, which
`builder/ibuilder.hpp`'s `framer(factory)` lets you supply.

## Do not call a blocking send from within a callback

`on_data`/`on_message` (and every other) callback runs on the channel's own
io thread. `send()`/`send_blocking()`/`send_move()`/`send_shared()` in the
default Reliable backpressure strategy block the calling thread until
backpressure clears - but clearing backpressure requires that same io
thread to keep making progress. Calling a blocking send from within a
callback while backpressure is active would deadlock the entire channel,
since the thread that would clear it is the one now waiting.

This applies to all seven wrappers and all user callback kinds: data,
message, both batch forms (including timer delivery), connect, disconnect,
error and backpressure. During any such callback, blocking-capable sends
return false immediately if they would need to wait for capacity, including
sends to a different channel. With capacity available, normal acceptance and
readiness checks still apply; callback scope alone does not reject a send.

The rule includes Reliable send()/send_line()/send_move()/send_shared(),
explicit send_blocking()/send_line_blocking(), and server Reliable
send_to()/send_to_line() and send_to_blocking(). Outside callbacks these
APIs retain their existing waiting behavior. The try_send*() APIs are unchanged.

For example:

```cpp
client->on_data([&](const wirestead::MessageContext& ctx) {
    // If backpressure happens to be active when this fires, send() returns
    // false right away rather than deadlocking - it does not block here.
    client->send("reply");
});
```

Always use the non-blocking `try_send()`/`try_send_move()`/`try_send_shared()`
API from within a callback - check the return value and handle a `false`
result (e.g. drop, retry later, or switch to `BackpressureStrategy::BestEffort`)
rather than relying on a blocking send inside callback context.

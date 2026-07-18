# Quick Start

This repository contains the minimal Wirestead quickstart. Extended tutorials
remain in the external documentation repository until that repository is moved.

## Minimal CMake

Create `CMakeLists.txt` next to `main.cpp`:

```cmake
cmake_minimum_required(VERSION 3.12)
project(wirestead_quickstart LANGUAGES CXX)

find_package(wirestead CONFIG REQUIRED)

add_executable(wirestead_quickstart main.cpp)
target_link_libraries(wirestead_quickstart PRIVATE wirestead::wirestead)
target_compile_features(wirestead_quickstart PRIVATE cxx_std_20)
```

## Minimal TCP client

This client expects a TCP server to be listening on `127.0.0.1:8080`.
For a runnable client/server pair, see the full tutorials in
the external documentation repository.

`max_retries` defaults to unlimited, so `start_sync()` blocks until a
connection actually succeeds or fails outright - it never returns `false`
on its own while the client keeps retrying an unreachable server. Set an
explicit `max_retries`/`connection_timeout` (as below) if you want
`start_sync()` to give up and return `false` after a bounded number of
attempts, which is almost always what you want for a first run or a
one-shot script.

```cpp
#include <chrono>
#include <iostream>
#include <wirestead/wirestead.hpp>

int main() {
    auto client = wirestead::tcp_client("127.0.0.1", 8080)
        .max_retries(3)
        .connection_timeout(std::chrono::seconds(2))
        .on_data([](const wirestead::MessageContext& ctx) {
            std::cout << "received " << ctx.data().size() << " bytes\n";
        })
        .on_error([](const wirestead::ErrorContext& ctx) {
            std::cerr << "error: " << ctx.message() << "\n";
        })
        .build();

    if (!client->start_sync()) {
        std::cerr << "failed to connect - is a server listening on 127.0.0.1:8080?\n";
        return 1;
    }

    client->send("hello");
    client->stop();
    return 0;
}
```

## Notes

- Callbacks are optional for construction.
- `on_error(...)` is recommended for production workflows.
- `ctx.data()` is a callback-scoped view. Copy data if it must outlive the callback.
- Use `try_send(...)` for non-blocking producer loops. See
  [Choosing a send method](error_model.md#choosing-a-send-method) for the
  full comparison of `send`/`try_send`/`send_blocking`/`send_move`/`send_shared`.
- Use `RuntimeStats` for diagnostics and queue/drop visibility.
- `max_retries` defaults to unlimited (`-1`) - set it explicitly if you need
  `start()`/`start_sync()` to eventually give up rather than retry forever.

Related docs:

- [Callback data lifetime](callbacks.md)
- [Error model](error_model.md)

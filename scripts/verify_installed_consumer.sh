#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CALLER_PWD="$(pwd)"

BUILD_DIR="${BUILD_DIR:-${PROJECT_ROOT}/build/consumer-smoke}"
INSTALL_PREFIX="${INSTALL_PREFIX:-${PROJECT_ROOT}/build/consumer-smoke-install}"
CONSUMER_DIR="${CONSUMER_DIR:-${PROJECT_ROOT}/build/consumer-smoke-app}"
LIBRARY_MODE="${LIBRARY_MODE:-shared}"

CMAKE_GENERATOR="${CMAKE_GENERATOR:-Ninja}"
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}"

usage() {
  cat <<'EOF'
Usage: scripts/verify_installed_consumer.sh [options]

Options:
  --build-dir PATH          Build directory for wirestead
  --install-prefix PATH     Temporary install prefix
  --consumer-dir PATH       External consumer project directory
  --library-mode MODE       shared, static, or both
  --help                    Show this help message
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

log_step() {
  echo
  echo "==> $*"
}

require_arg() {
  local option="$1"
  local value="${2:-}"
  if [[ -z "$value" || "$value" == --* ]]; then
    die "$option requires a value"
  fi
}

make_absolute() {
  local path="$1"
  if [[ "$path" = /* ]]; then
    echo "$path"
  else
    echo "${CALLER_PWD}/${path}"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir)
      require_arg "$1" "${2:-}"
      BUILD_DIR="$2"
      shift 2
      ;;
    --install-prefix)
      require_arg "$1" "${2:-}"
      INSTALL_PREFIX="$2"
      shift 2
      ;;
    --consumer-dir)
      require_arg "$1" "${2:-}"
      CONSUMER_DIR="$2"
      shift 2
      ;;
    --library-mode)
      require_arg "$1" "${2:-}"
      LIBRARY_MODE="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

case "$LIBRARY_MODE" in
  shared)
    WIRESTEAD_BUILD_SHARED=ON
    WIRESTEAD_BUILD_STATIC=OFF
    ;;
  static)
    WIRESTEAD_BUILD_SHARED=OFF
    WIRESTEAD_BUILD_STATIC=ON
    ;;
  both)
    WIRESTEAD_BUILD_SHARED=ON
    WIRESTEAD_BUILD_STATIC=ON
    ;;
  *)
    die "invalid --library-mode: $LIBRARY_MODE (expected shared, static, or both)"
    ;;
esac

BUILD_DIR="$(make_absolute "$BUILD_DIR")"
INSTALL_PREFIX="$(make_absolute "$INSTALL_PREFIX")"
CONSUMER_DIR="$(make_absolute "$CONSUMER_DIR")"

log_step "Preparing directories"
echo "Project root:     $PROJECT_ROOT"
echo "Build dir:        $BUILD_DIR"
echo "Install prefix:   $INSTALL_PREFIX"
echo "Consumer dir:     $CONSUMER_DIR"
echo "Library mode:     $LIBRARY_MODE"
echo "CMake generator:  $CMAKE_GENERATOR"
echo "CMake build type: $CMAKE_BUILD_TYPE"

rm -rf "$BUILD_DIR" "$INSTALL_PREFIX" "$CONSUMER_DIR"
mkdir -p "$BUILD_DIR" "$INSTALL_PREFIX" "$CONSUMER_DIR"

configure_args=(
  -S "$PROJECT_ROOT"
  -B "$BUILD_DIR"
  -G "$CMAKE_GENERATOR"
  -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE"
  -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX"
  -DWIRESTEAD_BUILD_SHARED="$WIRESTEAD_BUILD_SHARED"
  -DWIRESTEAD_BUILD_STATIC="$WIRESTEAD_BUILD_STATIC"
  -DWIRESTEAD_BUILD_TESTS=OFF
  -DWIRESTEAD_BUILD_DOCS=OFF
  -DWIRESTEAD_ENABLE_INSTALL=ON
  -DWIRESTEAD_ENABLE_PKGCONFIG=ON
  -DWIRESTEAD_ENABLE_EXPORT_HEADER=ON
)

if [[ -n "${CMAKE_TOOLCHAIN_FILE:-}" ]]; then
  configure_args+=("-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")
fi

if [[ -n "${VCPKG_TARGET_TRIPLET:-}" ]]; then
  configure_args+=("-DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}")
fi

log_step "Configuring wirestead"
cmake "${configure_args[@]}"

log_step "Building wirestead"
cmake --build "$BUILD_DIR" --parallel

log_step "Installing wirestead"
cmake --install "$BUILD_DIR"

log_step "Generating external consumer project"
cat > "$CONSUMER_DIR/CMakeLists.txt" <<'EOF'
cmake_minimum_required(VERSION 3.12)
project(wirestead_consumer_smoke LANGUAGES CXX)

find_package(wirestead CONFIG REQUIRED)

add_executable(wirestead_consumer_smoke main.cpp)
target_link_libraries(wirestead_consumer_smoke PRIVATE wirestead::wirestead)
target_compile_features(wirestead_consumer_smoke PRIVATE cxx_std_20)
EOF

cat > "$CONSUMER_DIR/main.cpp" <<'EOF'
#include <atomic>
#include <cstdint>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <wirestead/wirestead.hpp>

#ifndef _WIN32
uint16_t reserve_tcp_port() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return 0;
    }

    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(fd);
        return 0;
    }

    const auto port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}
#else
uint16_t reserve_tcp_port() {
    return static_cast<uint16_t>(42000 + (::GetCurrentProcessId() % 10000));
}
#endif

template <typename Predicate>
bool wait_until(Predicate&& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

int main() {
    const auto port = reserve_tcp_port();
    if (port == 0) {
        std::cerr << "failed to reserve a TCP loopback port\n";
        return 2;
    }

    std::atomic<int> server_received{0};
    std::atomic<int> client_received{0};

    std::shared_ptr<wirestead::TcpServer> tcp_server;
    tcp_server = wirestead::tcp_server(port)
        .auto_start(false)
        .on_data([&](const wirestead::MessageContext& ctx) {
            server_received.fetch_add(1);
            if (tcp_server) tcp_server->send_to(ctx.client_id(), "pong");
        })
        .on_error([](const wirestead::ErrorContext&) {})
        .build();
    if (!tcp_server || !tcp_server->start_sync()) {
        std::cerr << "failed to start installed TCP server\n";
        return 3;
    }

    auto tcp_client = wirestead::tcp_client("127.0.0.1", port)
        .auto_start(false)
        .on_data([&](const wirestead::MessageContext&) { client_received.fetch_add(1); })
        .on_error([](const wirestead::ErrorContext&) {})
        .build();
    if (!tcp_client || !tcp_client->start_sync()) {
        std::cerr << "failed to start installed TCP client\n";
        tcp_server->stop();
        return 4;
    }

    if (!wait_until([&]() { return tcp_client->connected() && tcp_server->client_count() >= 1; },
                    std::chrono::seconds(5))) {
        std::cerr << "installed TCP loopback did not connect\n";
        tcp_client->stop();
        tcp_server->stop();
        return 5;
    }

    if (!tcp_client->send("ping")) {
        std::cerr << "installed TCP client send failed\n";
        tcp_client->stop();
        tcp_server->stop();
        return 6;
    }

    if (!wait_until([&]() { return server_received.load() >= 1 && client_received.load() >= 1; },
                    std::chrono::seconds(5))) {
        std::cerr << "installed TCP request/reply did not complete\n";
        tcp_client->stop();
        tcp_server->stop();
        return 7;
    }

    if (!tcp_server->broadcast("broadcast")) {
        std::cerr << "installed TCP broadcast failed\n";
        tcp_client->stop();
        tcp_server->stop();
        return 8;
    }

    if (!wait_until([&]() { return client_received.load() >= 2; }, std::chrono::seconds(5))) {
        std::cerr << "installed TCP broadcast was not received\n";
        tcp_client->stop();
        tcp_server->stop();
        return 9;
    }

    const int server_before_stop = server_received.load();
    const int client_before_stop = client_received.load();
    tcp_client->stop();
    tcp_server->stop();
    tcp_client->send("late-client-send");
    tcp_server->broadcast("late-server-broadcast");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (server_received.load() != server_before_stop || client_received.load() != client_before_stop) {
        std::cerr << "callback fired after stop\n";
        return 10;
    }

    auto udp_client = wirestead::udp_client(0)
        .remote("127.0.0.1", 9000)
        .auto_start(false)
        .build();

    auto udp_server = wirestead::udp_server(9000).auto_start(false).build();

#ifndef _WIN32
    auto serial = wirestead::serial("/dev/null", 115200).auto_start(false).build();
    auto uds_client = wirestead::uds_client("/tmp/wirestead-consumer-smoke.sock").auto_start(false).build();
    auto uds_server = wirestead::uds_server("/tmp/wirestead-consumer-smoke.sock").auto_start(false).build();

    return (tcp_client && tcp_server && udp_client && udp_server &&
            serial && uds_client && uds_server) ? 0 : 1;
#else
    return (tcp_client && tcp_server && udp_client && udp_server) ? 0 : 1;
#endif
}
EOF

consumer_build_dir="$CONSUMER_DIR/build"

consumer_args=(
  -S "$CONSUMER_DIR"
  -B "$consumer_build_dir"
  -G "$CMAKE_GENERATOR"
  -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE"
  -DCMAKE_PREFIX_PATH="$INSTALL_PREFIX"
)

if [[ -n "${CMAKE_TOOLCHAIN_FILE:-}" ]]; then
  consumer_args+=("-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")
fi

if [[ -n "${VCPKG_TARGET_TRIPLET:-}" ]]; then
  consumer_args+=("-DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}")
fi

log_step "Configuring external consumer"
cmake "${consumer_args[@]}"

log_step "Building external consumer"
cmake --build "$consumer_build_dir" --parallel

log_step "Running external consumer runtime smoke"
if [[ -n "${LD_LIBRARY_PATH:-}" ]]; then
  export LD_LIBRARY_PATH="$INSTALL_PREFIX/lib:$LD_LIBRARY_PATH"
else
  export LD_LIBRARY_PATH="$INSTALL_PREFIX/lib"
fi
"$consumer_build_dir/wirestead_consumer_smoke"

echo
echo "Installed consumer smoke passed for library mode: $LIBRARY_MODE"

legacy_consumer_dir="${CONSUMER_DIR}-legacy-unilink"
rm -rf "$legacy_consumer_dir"
mkdir -p "$legacy_consumer_dir"

log_step "Generating find_package(unilink) legacy consumer project"
cat > "$legacy_consumer_dir/CMakeLists.txt" <<'EOF'
cmake_minimum_required(VERSION 3.12)
project(unilink_consumer_smoke LANGUAGES CXX)

find_package(unilink CONFIG REQUIRED)

add_executable(unilink_consumer_smoke main.cpp)
target_link_libraries(unilink_consumer_smoke PRIVATE unilink::unilink)
target_compile_features(unilink_consumer_smoke PRIVATE cxx_std_20)
EOF

cat > "$legacy_consumer_dir/main.cpp" <<'EOF'
#include <unilink/unilink.hpp>

int main() {
    // Port is never bound (auto_start(false), never start_sync()'d) - any
    // valid port number satisfies this build()-only smoke check.
    auto tcp_server = unilink::tcp_server(45678).auto_start(false).build();
    return tcp_server ? 0 : 1;
}
EOF

legacy_consumer_build_dir="$legacy_consumer_dir/build"

legacy_consumer_args=(
  -S "$legacy_consumer_dir"
  -B "$legacy_consumer_build_dir"
  -G "$CMAKE_GENERATOR"
  -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE"
  -DCMAKE_PREFIX_PATH="$INSTALL_PREFIX"
)

if [[ -n "${CMAKE_TOOLCHAIN_FILE:-}" ]]; then
  legacy_consumer_args+=("-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")
fi

if [[ -n "${VCPKG_TARGET_TRIPLET:-}" ]]; then
  legacy_consumer_args+=("-DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}")
fi

log_step "Configuring find_package(unilink) legacy consumer"
cmake "${legacy_consumer_args[@]}"

log_step "Building find_package(unilink) legacy consumer"
cmake --build "$legacy_consumer_build_dir" --parallel

log_step "Running find_package(unilink) legacy consumer runtime smoke"
"$legacy_consumer_build_dir/unilink_consumer_smoke"

echo
echo "Installed find_package(unilink) legacy consumer smoke passed for library mode: $LIBRARY_MODE"

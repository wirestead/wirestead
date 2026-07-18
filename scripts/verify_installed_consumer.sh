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
  --build-dir PATH          Build directory for unilink
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
    UNILINK_BUILD_SHARED=ON
    UNILINK_BUILD_STATIC=OFF
    ;;
  static)
    UNILINK_BUILD_SHARED=OFF
    UNILINK_BUILD_STATIC=ON
    ;;
  both)
    UNILINK_BUILD_SHARED=ON
    UNILINK_BUILD_STATIC=ON
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
  -DUNILINK_BUILD_SHARED="$UNILINK_BUILD_SHARED"
  -DUNILINK_BUILD_STATIC="$UNILINK_BUILD_STATIC"
  -DUNILINK_BUILD_TESTS=OFF
  -DUNILINK_BUILD_DOCS=OFF
  -DUNILINK_ENABLE_INSTALL=ON
  -DUNILINK_ENABLE_PKGCONFIG=ON
  -DUNILINK_ENABLE_EXPORT_HEADER=ON
)

if [[ -n "${CMAKE_TOOLCHAIN_FILE:-}" ]]; then
  configure_args+=("-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")
fi

if [[ -n "${VCPKG_TARGET_TRIPLET:-}" ]]; then
  configure_args+=("-DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}")
fi

log_step "Configuring unilink"
cmake "${configure_args[@]}"

log_step "Building unilink"
cmake --build "$BUILD_DIR" --parallel

log_step "Installing unilink"
cmake --install "$BUILD_DIR"

log_step "Generating external consumer project"
cat > "$CONSUMER_DIR/CMakeLists.txt" <<'EOF'
cmake_minimum_required(VERSION 3.12)
project(unilink_consumer_smoke LANGUAGES CXX)

find_package(unilink CONFIG REQUIRED)

add_executable(unilink_consumer_smoke main.cpp)
target_link_libraries(unilink_consumer_smoke PRIVATE unilink::unilink)
target_compile_features(unilink_consumer_smoke PRIVATE cxx_std_20)
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

#include <unilink/unilink.hpp>

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

    std::shared_ptr<unilink::TcpServer> tcp_server;
    tcp_server = unilink::tcp_server(port)
        .auto_start(false)
        .on_data([&](const unilink::MessageContext& ctx) {
            server_received.fetch_add(1);
            if (tcp_server) tcp_server->send_to(ctx.client_id(), "pong");
        })
        .on_error([](const unilink::ErrorContext&) {})
        .build();
    if (!tcp_server || !tcp_server->start_sync()) {
        std::cerr << "failed to start installed TCP server\n";
        return 3;
    }

    auto tcp_client = unilink::tcp_client("127.0.0.1", port)
        .auto_start(false)
        .on_data([&](const unilink::MessageContext&) { client_received.fetch_add(1); })
        .on_error([](const unilink::ErrorContext&) {})
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

    auto udp_client = unilink::udp_client(0)
        .remote("127.0.0.1", 9000)
        .auto_start(false)
        .build();

    auto udp_server = unilink::udp_server(9000).auto_start(false).build();

#ifndef _WIN32
    auto serial = unilink::serial("/dev/null", 115200).auto_start(false).build();
    auto uds_client = unilink::uds_client("/tmp/unilink-consumer-smoke.sock").auto_start(false).build();
    auto uds_server = unilink::uds_server("/tmp/unilink-consumer-smoke.sock").auto_start(false).build();

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
"$consumer_build_dir/unilink_consumer_smoke"

echo
echo "Installed consumer smoke passed for library mode: $LIBRARY_MODE"

wirestead_consumer_dir="${CONSUMER_DIR}-wirestead"
rm -rf "$wirestead_consumer_dir"
mkdir -p "$wirestead_consumer_dir"

log_step "Generating find_package(wirestead) consumer project"
cat > "$wirestead_consumer_dir/CMakeLists.txt" <<'EOF'
cmake_minimum_required(VERSION 3.12)
project(wirestead_consumer_smoke LANGUAGES CXX)

find_package(wirestead CONFIG REQUIRED)

add_executable(wirestead_consumer_smoke main.cpp)
target_link_libraries(wirestead_consumer_smoke PRIVATE wirestead::wirestead)
target_compile_features(wirestead_consumer_smoke PRIVATE cxx_std_20)
EOF

# Headers still live under <unilink/...> at this stage of the migration (see
# docs/migration-from-unilink.md) - this only proves the wirestead::wirestead
# target resolves to the real library and links, not the header story.
cat > "$wirestead_consumer_dir/main.cpp" <<'EOF'
#include <unilink/unilink.hpp>

int main() {
    auto tcp_server = unilink::tcp_server(0).auto_start(false).build();
    return tcp_server ? 0 : 1;
}
EOF

wirestead_consumer_build_dir="$wirestead_consumer_dir/build"

wirestead_consumer_args=(
  -S "$wirestead_consumer_dir"
  -B "$wirestead_consumer_build_dir"
  -G "$CMAKE_GENERATOR"
  -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE"
  -DCMAKE_PREFIX_PATH="$INSTALL_PREFIX"
)

if [[ -n "${CMAKE_TOOLCHAIN_FILE:-}" ]]; then
  wirestead_consumer_args+=("-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")
fi

if [[ -n "${VCPKG_TARGET_TRIPLET:-}" ]]; then
  wirestead_consumer_args+=("-DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}")
fi

log_step "Configuring find_package(wirestead) consumer"
cmake "${wirestead_consumer_args[@]}"

log_step "Building find_package(wirestead) consumer"
cmake --build "$wirestead_consumer_build_dir" --parallel

log_step "Running find_package(wirestead) consumer runtime smoke"
"$wirestead_consumer_build_dir/wirestead_consumer_smoke"

echo
echo "Installed find_package(wirestead) consumer smoke passed for library mode: $LIBRARY_MODE"

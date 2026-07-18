# Installation

Full installation guide:

https://github.com/unilink-lab/unilink-docs/blob/main/docs/user/installation.md

## Requirements

- C++20 compiler
- CMake 3.12+ for plain builds
- CMake 3.21+ for repository presets
- Boost 1.83.0+
- spdlog dependency according to build configuration

vcpkg is the recommended dependency supplier. CMake owns the dependency version
gate and rejects Boost versions older than the configured minimum.

## vcpkg

```bash
vcpkg install jwsung91-unilink
```

## Minimal CMake find_package consumer

```cmake
cmake_minimum_required(VERSION 3.12)
project(my_app LANGUAGES CXX)

find_package(unilink CONFIG REQUIRED)

add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE unilink::unilink)
target_compile_features(my_app PRIVATE cxx_std_20)
```

## Include

```cpp
#include <unilink/unilink.hpp>
```

## Source build example

Source builds require Boost 1.83.0+. Ubuntu 22.04 and 24.04 system Boost
packages may be older than this baseline, so prefer the repository development
setup or provide Boost through vcpkg/custom CMake prefixes before configuring.

For contributor/source builds with the repository-managed vcpkg checkout:

```bash
./scripts/setup_dev_env.sh
cmake --preset dev-linux-x64
cmake --build --preset dev-linux-x64 --parallel 1
```

For a plain source install with an existing dependency setup:

```bash
git clone https://github.com/wirestead/wirestead.git
cd wirestead
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel 1
sudo cmake --install build
```

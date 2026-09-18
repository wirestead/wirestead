# Installation

This repository contains the canonical source-build and package-consumer
installation notes for Wirestead.

## Requirements

- C++20 compiler
- CMake 3.12+ for plain builds
- CMake 3.21+ for repository presets
- Boost 1.74.0+
- spdlog dependency according to build configuration

vcpkg is the recommended dependency supplier. CMake owns the dependency version
gate and rejects Boost versions older than the configured minimum.

## vcpkg

```bash
vcpkg install wirestead
```

For Unilink migration details, see
[Migrating from Unilink](migration-from-unilink.md).

## Minimal CMake find_package consumer

```cmake
cmake_minimum_required(VERSION 3.12)
project(my_app LANGUAGES CXX)

find_package(wirestead CONFIG REQUIRED)

add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE wirestead::wirestead)
target_compile_features(my_app PRIVATE cxx_std_20)
```

## Include

```cpp
#include <wirestead/wirestead.hpp>
```

## Source build example

Source builds require Boost 1.74.0+, which the system packages on Ubuntu 22.04
(1.74), RHEL 9 (1.75) and Ubuntu 24.04 (1.83) all satisfy. vcpkg or a custom
CMake prefix is still the way to build against a newer Boost than the
distribution supplies.

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

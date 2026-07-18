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

#if __has_include(<unilink_export.hpp>)
#include <unilink_export.hpp>
#endif

#if !defined(UNILINK_API)
#if defined(UNILINK_BUILD_SHARED)
#if defined(_WIN32) || defined(__CYGWIN__)
#if defined(UNILINK_BUILDING_LIBRARY)
#define UNILINK_API __declspec(dllexport)
#else
#define UNILINK_API __declspec(dllimport)
#endif
#else
#define UNILINK_API __attribute__((visibility("default")))
#endif
#elif defined(UNILINK_EXPORT)
#define UNILINK_API UNILINK_EXPORT
#else
#define UNILINK_API
#endif
#endif

#if !defined(UNILINK_LOCAL)
#if defined(UNILINK_NO_EXPORT)
#define UNILINK_LOCAL UNILINK_NO_EXPORT
#elif defined(_WIN32) || defined(__CYGWIN__)
#define UNILINK_LOCAL
#else
#define UNILINK_LOCAL __attribute__((visibility("hidden")))
#endif
#endif

#ifndef UNILINK_EXPORT
#define UNILINK_EXPORT UNILINK_API
#endif

#ifndef UNILINK_NO_EXPORT
#define UNILINK_NO_EXPORT UNILINK_LOCAL
#endif

// Wirestead alias: every symbol is annotated with UNILINK_API, not a
// separately-tracked macro, so WIRESTEAD_API always mirrors whatever
// UNILINK_API resolved to above rather than duplicating the dllexport/
// dllimport/visibility logic. See docs/migration-from-unilink.md.
#ifndef WIRESTEAD_API
#define WIRESTEAD_API UNILINK_API
#endif

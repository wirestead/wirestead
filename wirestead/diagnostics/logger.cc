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

#include "logger.hpp"

#include <spdlog/async.h>
#include <spdlog/details/thread_pool.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/sinks/dist_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <ctime>
#include <future>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string_view>

namespace wirestead {
namespace diagnostics {

/**
 * @brief Custom spdlog sink for LogCallback
 */
template <typename Mutex>
class callback_sink : public spdlog::sinks::base_sink<Mutex> {
 public:
  explicit callback_sink(Logger::LogCallback callback) : callback_(std::move(callback)) {}

  void set_callback(Logger::LogCallback callback) {
    std::lock_guard<Mutex> lock(spdlog::sinks::base_sink<Mutex>::mutex_);
    callback_ = std::move(callback);
  }

 protected:
  void sink_it_(const spdlog::details::log_msg& msg) override {
    if (!callback_) return;

    spdlog::memory_buf_t formatted;
    spdlog::sinks::base_sink<Mutex>::formatter_->format(msg, formatted);
    try {
      callback_(from_spdlog_level(msg.level), fmt::to_string(formatted));
    } catch (...) {
    }
  }

  void flush_() override {}

 private:
  static LogLevel from_spdlog_level(spdlog::level::level_enum level) {
    switch (level) {
      case spdlog::level::debug:
        return LogLevel::DEBUG;
      case spdlog::level::info:
        return LogLevel::INFO;
      case spdlog::level::warn:
        return LogLevel::WARNING;
      case spdlog::level::err:
        return LogLevel::ERROR;
      case spdlog::level::critical:
        return LogLevel::CRITICAL;
      default:
        return LogLevel::INFO;
    }
  }

  Logger::LogCallback callback_;
};

using callback_sink_mt = callback_sink<std::mutex>;

class level_range_sink final : public spdlog::sinks::sink {
 public:
  level_range_sink(std::shared_ptr<spdlog::sinks::sink> sink, spdlog::level::level_enum min_level,
                   spdlog::level::level_enum max_level)
      : sink_(std::move(sink)), min_level_(min_level), max_level_(max_level) {}

  void log(const spdlog::details::log_msg& msg) override {
    if (msg.level >= min_level_ && msg.level <= max_level_) {
      sink_->log(msg);
    }
  }

  void flush() override { sink_->flush(); }

  void set_pattern(const std::string& pattern) override { sink_->set_pattern(pattern); }

  void set_formatter(std::unique_ptr<spdlog::formatter> sink_formatter) override {
    sink_->set_formatter(std::move(sink_formatter));
  }

 private:
  std::shared_ptr<spdlog::sinks::sink> sink_;
  spdlog::level::level_enum min_level_;
  spdlog::level::level_enum max_level_;
};

struct Logger::Impl {
  mutable std::mutex mutex_;
  std::atomic<LogLevel> current_level_{LogLevel::INFO};
  std::atomic<bool> enabled_{true};
  std::atomic<int> outputs_{static_cast<int>(LogOutput::CONSOLE)};

  std::shared_ptr<spdlog::logger> spd_logger_;
  std::shared_ptr<spdlog::sinks::dist_sink_mt> dist_sink_;
  std::shared_ptr<spdlog::sinks::stdout_color_sink_mt> console_stdout_sink_;
  std::shared_ptr<spdlog::sinks::stderr_color_sink_mt> console_stderr_sink_;
  std::shared_ptr<level_range_sink> console_stdout_filter_;
  std::shared_ptr<level_range_sink> console_stderr_filter_;
  std::shared_ptr<spdlog::sinks::rotating_file_sink_mt> file_sink_;
  std::shared_ptr<callback_sink_mt> callback_sink_;

  std::string current_log_file_;
  LogRotationConfig rotation_config_;
  std::string format_ = "{timestamp} [{level}] [{component}] [{operation}] [{source}] {message}";
  std::string last_error_;

  // Async logging support
  std::atomic<bool> async_enabled_{false};
  AsyncLogConfig async_config_;
  std::future<void> drop_future_;
  std::shared_ptr<spdlog::details::thread_pool> async_thread_pool_;

  Impl() {
    dist_sink_ = std::make_shared<spdlog::sinks::dist_sink_mt>();

    // Default to sync logger initially, can be changed to async via set_async_logging
    spd_logger_ = std::make_shared<spdlog::logger>("wirestead", dist_sink_);
    spd_logger_->set_level(to_spdlog_level(LogLevel::DEBUG));  // Allow all, filter via Logger::log or Logger::set_level

    add_console_sinks();

    apply_spdlog_pattern();
    apply_environment_settings();
  }

  ~Impl() {
    if (drop_future_.valid()) {
      drop_future_.wait();
    }
    spdlog::drop("wirestead");
  }

  void apply_spdlog_pattern() {
    if (dist_sink_) {
      dist_sink_->set_pattern("%v");
    }
  }

  void set_error(std::string message) { last_error_ = std::move(message); }

  void clear_error() { last_error_.clear(); }

  bool has_outputs_unlocked() const { return outputs_.load() != 0; }

  void add_console_sinks() {
    if (!console_stdout_sink_) {
      console_stdout_sink_ = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
      console_stdout_filter_ =
          std::make_shared<level_range_sink>(console_stdout_sink_, spdlog::level::trace, spdlog::level::warn);
    }
    if (!console_stderr_sink_) {
      console_stderr_sink_ = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
      console_stderr_filter_ =
          std::make_shared<level_range_sink>(console_stderr_sink_, spdlog::level::err, spdlog::level::critical);
    }

    dist_sink_->remove_sink(console_stdout_filter_);
    dist_sink_->remove_sink(console_stderr_filter_);
    dist_sink_->add_sink(console_stdout_filter_);
    dist_sink_->add_sink(console_stderr_filter_);
    apply_spdlog_pattern();
  }

  void remove_console_sinks() {
    if (console_stdout_filter_) {
      dist_sink_->remove_sink(console_stdout_filter_);
    }
    if (console_stderr_filter_) {
      dist_sink_->remove_sink(console_stderr_filter_);
    }
    console_stdout_filter_.reset();
    console_stderr_filter_.reset();
    console_stdout_sink_.reset();
    console_stderr_sink_.reset();
  }

  void set_format(const std::string& format) { format_ = format; }

  static void replace_all(std::string& str, const std::string& from, const std::string& to) {
    if (from.empty()) {
      return;
    }

    size_t pos = 0;
    while ((pos = str.find(from, pos)) != std::string::npos) {
      str.replace(pos, from.length(), to);
      pos += to.length();
    }
  }

  static std::string level_name(LogLevel level) {
    switch (level) {
      case LogLevel::DEBUG:
        return "debug";
      case LogLevel::INFO:
        return "info";
      case LogLevel::WARNING:
        return "warning";
      case LogLevel::ERROR:
        return "error";
      case LogLevel::CRITICAL:
        return "critical";
      default:
        return "info";
    }
  }

  static std::string normalize_env_value(std::string_view value) {
    std::string normalized(value);
    normalized.erase(normalized.begin(), std::find_if(normalized.begin(), normalized.end(),
                                                      [](unsigned char c) { return !std::isspace(c); }));
    normalized.erase(
        std::find_if(normalized.rbegin(), normalized.rend(), [](unsigned char c) { return !std::isspace(c); }).base(),
        normalized.end());
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return normalized;
  }

  static bool parse_log_level(std::string_view value, LogLevel& level, bool& disable_logging) {
    const auto normalized = normalize_env_value(value);
    disable_logging = false;
    if (normalized == "DEBUG" || normalized == "TRACE") {
      level = LogLevel::DEBUG;
      return true;
    }
    if (normalized == "INFO") {
      level = LogLevel::INFO;
      return true;
    }
    if (normalized == "WARNING" || normalized == "WARN") {
      level = LogLevel::WARNING;
      return true;
    }
    if (normalized == "ERROR" || normalized == "ERR") {
      level = LogLevel::ERROR;
      return true;
    }
    if (normalized == "CRITICAL" || normalized == "FATAL") {
      level = LogLevel::CRITICAL;
      return true;
    }
    if (normalized == "OFF" || normalized == "NONE" || normalized == "DISABLED") {
      disable_logging = true;
      return true;
    }
    return false;
  }

  void apply_environment_settings() {
    // WIRESTEAD_LOG_LEVEL takes priority over UNILINK_LOG_LEVEL when both are
    // set (docs/migration-from-unilink.md compatibility policy), with a
    // startup log line so the choice is discoverable rather than silent.
    const char* wirestead_env = std::getenv("WIRESTEAD_LOG_LEVEL");
    const char* unilink_env = std::getenv("UNILINK_LOG_LEVEL");
    const bool has_wirestead_env = wirestead_env && *wirestead_env != '\0';
    const bool has_unilink_env = unilink_env && *unilink_env != '\0';

    const char* env_level = has_wirestead_env ? wirestead_env : unilink_env;
    const char* env_name = has_wirestead_env ? "WIRESTEAD_LOG_LEVEL" : "UNILINK_LOG_LEVEL";
    if (!env_level || *env_level == '\0') {
      return;
    }

    if (has_wirestead_env && has_unilink_env && spd_logger_) {
      spd_logger_->info("WIRESTEAD_LOG_LEVEL is set alongside UNILINK_LOG_LEVEL; WIRESTEAD_LOG_LEVEL takes precedence");
    }

    LogLevel parsed_level = current_level_.load();
    bool disable_logging = false;
    if (!parse_log_level(env_level, parsed_level, disable_logging)) {
      set_error("Invalid " + std::string(env_name) + ": " + std::string(env_level));
      return;
    }

    enabled_.store(!disable_logging);
    if (!disable_logging) {
      current_level_.store(parsed_level);
      if (spd_logger_) {
        spd_logger_->set_level(to_spdlog_level(parsed_level));
      }
    }
    clear_error();
  }

  static std::string timestamp_now() {
    using namespace std::chrono;
    const auto now = system_clock::now();
    const auto tt = system_clock::to_time_t(now);
    std::tm tm{};
#if defined(_WIN32)
    localtime_s(&tm, &tt);
#else
    localtime_r(&tt, &tm);
#endif
    const auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

    std::ostringstream oss;
    oss << std::put_time(&tm, "%F %T") << '.' << std::setw(3) << std::setfill('0') << ms.count();
    return oss.str();
  }

  static std::string format_message(const std::string& format, LogLevel level, std::string_view component,
                                    std::string_view operation, std::string_view message,
                                    const std::source_location& loc) {
    const auto file = std::string(loc.file_name());
    const auto line = std::to_string(loc.line());
    const auto function = std::string(loc.function_name());
    const auto source = fmt::format("{}:{}:{}", file, line, function);

    std::string formatted = format;
    replace_all(formatted, "{timestamp}", timestamp_now());
    replace_all(formatted, "{level}", level_name(level));
    replace_all(formatted, "{component}", std::string(component));
    replace_all(formatted, "{operation}", std::string(operation));
    replace_all(formatted, "{source}", source);
    replace_all(formatted, "{file}", file);
    replace_all(formatted, "{line}", line);
    replace_all(formatted, "{function}", function);
    replace_all(formatted, "{message}", std::string(message));
    return formatted;
  }

  void flush() {
    auto logger = spd_logger_;
    auto sink = dist_sink_;
    if (logger) logger->flush();
    if (sink) sink->flush();
  }

  void setup_async_logging(const AsyncLogConfig& config) {
    async_config_ = config;

    spdlog::drop("wirestead");  // Drop existing logger

    auto overflow_policy = config.enable_backpressure ? spdlog::async_overflow_policy::block
                                                      : spdlog::async_overflow_policy::overrun_oldest;

    async_thread_pool_ = std::make_shared<spdlog::details::thread_pool>(std::max<size_t>(1, config.max_queue_size), 1);

    spd_logger_ = std::make_shared<spdlog::async_logger>("wirestead", dist_sink_, async_thread_pool_, overflow_policy);
    spd_logger_->set_level(to_spdlog_level(current_level_.load()));
    apply_spdlog_pattern();

    spdlog::register_logger(spd_logger_);

    if (config.flush_interval.count() > 0) {
      // spdlog::flush_every takes std::chrono::seconds.
      // Ensure at least 1 second if a positive interval is requested.
      auto secs =
          std::max(std::chrono::seconds(1), std::chrono::duration_cast<std::chrono::seconds>(config.flush_interval));
      spdlog::flush_every(secs);
    }

    async_enabled_.store(true);
  }

  void teardown_async_logging() {
    if (!async_enabled_.load()) return;

    async_enabled_.store(false);

    // If there's a previous drop still pending, wait for it now to avoid multiple drop tasks
    if (drop_future_.valid()) {
      drop_future_.wait();
    }

    // Use std::async to drop the logger with a timeout
    drop_future_ = std::async(std::launch::async, []() { spdlog::drop("wirestead"); });

    bool drop_success = true;
    if (drop_future_.wait_for(async_config_.shutdown_timeout) == std::future_status::timeout) {
      drop_success = false;
    }

    spd_logger_.reset();
    if (drop_success) {
      async_thread_pool_.reset();
    }
    dist_sink_->flush();

    // Create sync logger
    spd_logger_ = std::make_shared<spdlog::logger>("wirestead", dist_sink_);
    spd_logger_->set_level(to_spdlog_level(current_level_.load()));
    apply_spdlog_pattern();

    // Only register globally if the previous drop finished, to avoid race conditions.
    // If it didn't finish, we still have spd_logger_ internally so log() works.
    if (drop_success) {
      spdlog::register_logger(spd_logger_);
    }
  }

  static spdlog::level::level_enum to_spdlog_level(LogLevel level) {
    switch (level) {
      case LogLevel::DEBUG:
        return spdlog::level::debug;
      case LogLevel::INFO:
        return spdlog::level::info;
      case LogLevel::WARNING:
        return spdlog::level::warn;
      case LogLevel::ERROR:
        return spdlog::level::err;
      case LogLevel::CRITICAL:
        return spdlog::level::critical;
      default:
        return spdlog::level::info;
    }
  }
};

Logger::Logger() : impl_(std::make_unique<Impl>()) {}

Logger::~Logger() = default;

Logger::Logger(Logger&&) noexcept = default;
Logger& Logger::operator=(Logger&&) noexcept = default;

Logger& Logger::instance() {
  static Logger* inst = new Logger();
  return *inst;
}

Logger& Logger::default_logger() { return instance(); }

void Logger::set_level(LogLevel level) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  impl_->current_level_.store(level);
  if (impl_->spd_logger_) {
    impl_->spd_logger_->set_level(Impl::to_spdlog_level(level));
  }
}

LogLevel Logger::level() const { return get_impl()->current_level_.load(); }

bool Logger::should_log(LogLevel level) const {
  const auto* impl = get_impl();
  return impl->enabled_.load() && impl->has_outputs_unlocked() && level >= impl->current_level_.load();
}

void Logger::set_console_output(bool enable) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  if (enable) {
    impl_->add_console_sinks();
    impl_->outputs_.fetch_or(static_cast<int>(LogOutput::CONSOLE));
  } else {
    impl_->remove_console_sinks();
    impl_->outputs_.fetch_and(~static_cast<int>(LogOutput::CONSOLE));
  }
  impl_->clear_error();
}

void Logger::set_file_output(const std::string& filename) { (void)try_set_file_output_with_rotation(filename); }

bool Logger::try_set_file_output(const std::string& filename) { return try_set_file_output_with_rotation(filename); }

void Logger::set_file_output_with_rotation(const std::string& filename, const LogRotationConfig& config) {
  (void)try_set_file_output_with_rotation(filename, config);
}

bool Logger::try_set_file_output_with_rotation(const std::string& filename, const LogRotationConfig& config) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);

  if (filename.empty()) {
    if (impl_->file_sink_) {
      impl_->dist_sink_->remove_sink(impl_->file_sink_);
      impl_->file_sink_.reset();
    }
    impl_->current_log_file_.clear();
    impl_->outputs_.fetch_and(~static_cast<int>(LogOutput::FILE));
    impl_->clear_error();
    return true;
  } else {
    try {
      if (config.enable_compression) {
        impl_->set_error("Log compression is not supported by the current rotating file sink");
        return false;
      }
      if (config.file_pattern != LogRotationConfig{}.file_pattern) {
        impl_->set_error("Custom log rotation file_pattern is not supported by the current rotating file sink");
        return false;
      }
      if (impl_->file_sink_) {
        impl_->dist_sink_->remove_sink(impl_->file_sink_);
      }

      impl_->rotation_config_ = config;
      impl_->current_log_file_ = filename;

      impl_->file_sink_ = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(filename, config.max_file_size_bytes,
                                                                                 config.max_files);

      impl_->dist_sink_->add_sink(impl_->file_sink_);
      impl_->apply_spdlog_pattern();
      impl_->outputs_.fetch_or(static_cast<int>(LogOutput::FILE));
      impl_->clear_error();
      return true;
    } catch (const spdlog::spdlog_ex& e) {
      impl_->set_error("Failed to open log file: " + filename + " (" + e.what() + ")");
      std::cerr << impl_->last_error_ << std::endl;
      impl_->file_sink_.reset();
      impl_->current_log_file_.clear();
      impl_->outputs_.fetch_and(~static_cast<int>(LogOutput::FILE));
      return false;
    }
  }
}

void Logger::set_async_logging(bool enable, const AsyncLogConfig& config) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  if (enable) {
    if (impl_->async_enabled_.load()) {
      impl_->teardown_async_logging();
    }
    impl_->setup_async_logging(config);
  } else {
    impl_->teardown_async_logging();
  }
}

bool Logger::async_logging_enabled() const { return get_impl()->async_enabled_.load(); }

void Logger::reload_from_environment() {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  impl_->apply_environment_settings();
}

void Logger::set_callback(LogCallback callback) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);

  if (!callback) {
    if (impl_->callback_sink_) {
      impl_->dist_sink_->remove_sink(impl_->callback_sink_);
      impl_->callback_sink_.reset();
    }
    impl_->outputs_.fetch_and(~static_cast<int>(LogOutput::CALLBACK));
    impl_->clear_error();
    return;
  }

  if (impl_->callback_sink_) {
    impl_->callback_sink_->set_callback(std::move(callback));
    impl_->dist_sink_->remove_sink(impl_->callback_sink_);  // Avoid duplicates
    impl_->dist_sink_->add_sink(impl_->callback_sink_);
  } else {
    impl_->callback_sink_ = std::make_shared<callback_sink_mt>(std::move(callback));
    impl_->dist_sink_->add_sink(impl_->callback_sink_);
  }
  impl_->apply_spdlog_pattern();
  impl_->outputs_.fetch_or(static_cast<int>(LogOutput::CALLBACK));
  impl_->clear_error();
}

void Logger::set_outputs(int outputs) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  int effective_outputs = 0;

  // Reconcile sinks in dist_sink_ based on the new bitmask

  // Console Sink
  if (outputs & static_cast<int>(LogOutput::CONSOLE)) {
    impl_->add_console_sinks();
    effective_outputs |= static_cast<int>(LogOutput::CONSOLE);
  } else {
    impl_->remove_console_sinks();
  }

  // File Sink
  if (outputs & static_cast<int>(LogOutput::FILE)) {
    if (!impl_->file_sink_ && !impl_->current_log_file_.empty()) {
      impl_->file_sink_ = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
          impl_->current_log_file_, impl_->rotation_config_.max_file_size_bytes, impl_->rotation_config_.max_files);
    }
    if (impl_->file_sink_) {
      impl_->dist_sink_->remove_sink(impl_->file_sink_);  // Ensure no duplicate
      impl_->dist_sink_->add_sink(impl_->file_sink_);
      impl_->apply_spdlog_pattern();
      effective_outputs |= static_cast<int>(LogOutput::FILE);
    }
  } else {
    if (impl_->file_sink_) {
      impl_->dist_sink_->remove_sink(impl_->file_sink_);
      impl_->file_sink_.reset();
    }
  }

  // Callback Sink
  if (outputs & static_cast<int>(LogOutput::CALLBACK)) {
    if (impl_->callback_sink_) {
      impl_->dist_sink_->remove_sink(impl_->callback_sink_);  // Ensure no duplicate
      impl_->dist_sink_->add_sink(impl_->callback_sink_);
      impl_->apply_spdlog_pattern();
      effective_outputs |= static_cast<int>(LogOutput::CALLBACK);
    }
  } else {
    if (impl_->callback_sink_) {
      impl_->dist_sink_->remove_sink(impl_->callback_sink_);
    }
  }

  impl_->outputs_.store(effective_outputs);
  impl_->clear_error();
}

void Logger::set_enabled(bool enabled) { impl_->enabled_.store(enabled); }

bool Logger::enabled() const { return get_impl()->enabled_.load(); }

bool Logger::has_outputs() const { return get_impl()->has_outputs_unlocked(); }

std::string Logger::last_error() const {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  return impl_->last_error_;
}

void Logger::set_format(const std::string& format) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  impl_->set_format(format);
  impl_->clear_error();
}

void Logger::flush() {
  std::shared_ptr<spdlog::logger> logger;
  std::shared_ptr<spdlog::sinks::dist_sink_mt> sink;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    logger = impl_->spd_logger_;
    sink = impl_->dist_sink_;
  }
  if (logger) logger->flush();
  if (sink) sink->flush();
}

void Logger::log(LogLevel level, std::string_view component, std::string_view operation, std::string_view message,
                 const std::source_location& loc) {
  if (!should_log(level)) {
    return;
  }

  std::shared_ptr<spdlog::logger> logger;
  std::string format;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    logger = impl_->spd_logger_;
    format = impl_->format_;
  }

  if (!logger) {
    return;
  }

  const auto payload = Impl::format_message(format, level, component, operation, message, loc);
  logger->log(Impl::to_spdlog_level(level), payload);
}

void Logger::debug(std::string_view component, std::string_view operation, std::string_view message,
                   const std::source_location& loc) {
  log(LogLevel::DEBUG, component, operation, message, loc);
}

void Logger::info(std::string_view component, std::string_view operation, std::string_view message,
                  const std::source_location& loc) {
  log(LogLevel::INFO, component, operation, message, loc);
}

void Logger::warning(std::string_view component, std::string_view operation, std::string_view message,
                     const std::source_location& loc) {
  log(LogLevel::WARNING, component, operation, message, loc);
}

void Logger::error(std::string_view component, std::string_view operation, std::string_view message,
                   const std::source_location& loc) {
  log(LogLevel::ERROR, component, operation, message, loc);
}

void Logger::critical(std::string_view component, std::string_view operation, std::string_view message,
                      const std::source_location& loc) {
  log(LogLevel::CRITICAL, component, operation, message, loc);
}

}  // namespace diagnostics
}  // namespace wirestead

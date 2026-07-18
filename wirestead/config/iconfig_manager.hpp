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

#include <any>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "wirestead/base/visibility.hpp"

namespace wirestead {
namespace config {

/**
 * Configuration value types supported by the system
 */
enum class ConfigType { String, Integer, Boolean, Double, Array, Object };

/**
 * Configuration validation result
 */
struct ValidationResult {
  bool is_valid;
  std::string error_message;

  explicit ValidationResult(bool valid = true, const std::string& error = "") : is_valid(valid), error_message(error) {}

  static ValidationResult success() { return ValidationResult(true); }
  static ValidationResult error(const std::string& msg) { return ValidationResult(false, msg); }
};

/**
 * Configuration item definition
 */
struct ConfigItem {
  std::string key;
  std::any value;
  ConfigType type;
  bool required;
  std::string description;
  std::function<ValidationResult(const std::any&)> validator;

  // Default constructor
  ConfigItem() : key(""), value(std::any()), type(ConfigType::String), required(false), description("") {}

  ConfigItem(const std::string& k, const std::any& v, ConfigType t, bool req = false, const std::string& desc = "")
      : key(k), value(v), type(t), required(req), description(desc) {}
};

/**
 * Configuration change callback
 */
using ConfigChangeCallback =
    std::function<void(const std::string& key, const std::any& old_value, const std::any& new_value)>;

/**
 * Abstract interface for configuration management
 */
class WIRESTEAD_API ConfigManagerInterface {
 public:
  virtual ~ConfigManagerInterface() = default;

  // Configuration access
  virtual std::any get(const std::string& key) const = 0;
  virtual std::any get(const std::string& key, const std::any& default_value) const = 0;
  virtual bool has(const std::string& key) const = 0;

  // Configuration modification
  virtual ValidationResult set(const std::string& key, const std::any& value) = 0;
  virtual bool remove(const std::string& key) = 0;
  virtual void clear() = 0;

  // Configuration validation
  virtual ValidationResult validate() const = 0;
  virtual ValidationResult validate(const std::string& key) const = 0;

  // Configuration registration
  virtual void register_item(const ConfigItem& item) = 0;
  virtual void register_validator(const std::string& key,
                                  std::function<ValidationResult(const std::any&)> validator) = 0;

  // Change notifications
  virtual void on_change(const std::string& key, ConfigChangeCallback callback) = 0;
  virtual void remove_change_callback(const std::string& key) = 0;

  // Configuration persistence
  virtual bool save_to_file(const std::string& filepath) const = 0;
  virtual bool load_from_file(const std::string& filepath) = 0;

  // Configuration introspection
  virtual std::vector<std::string> get_keys() const = 0;
  virtual ConfigType get_type(const std::string& key) const = 0;
  virtual std::string get_description(const std::string& key) const = 0;
  virtual bool is_required(const std::string& key) const = 0;
};

}  // namespace config
}  // namespace wirestead

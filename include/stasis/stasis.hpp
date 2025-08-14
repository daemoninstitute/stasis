/*
 * Copyright 2025 James Wilson
 * SPDX-License-Identifier: Apache-2.0
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

#include <cstdint>
#include <expected>
#include <functional>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace stasis {

enum class AppError : std::uint8_t {
  KeyNotFound,
  NoActiveTransaction,
};

struct StringHash {
  using is_transparent = void;
  [[nodiscard]] auto operator()(const char *txt) const -> std::size_t {
    return std::hash<std::string_view>{}(txt);
  }
  [[nodiscard]] auto operator()(std::string_view txt) const -> std::size_t {
    return std::hash<std::string_view>{}(txt);
  }
  [[nodiscard]] auto operator()(const std::string &txt) const -> std::size_t {
    return std::hash<std::string>{}(txt);
  }
};

using TransactionChanges =
    std::unordered_map<std::string, std::optional<std::string>, StringHash,
                       std::equal_to<>>;

using MainStore =
    std::unordered_map<std::string, std::string, StringHash, std::equal_to<>>;

struct Key {
  std::string_view value;
};

struct Value {
  std::string_view value;
};

class KeyValueStore {
public:
  struct Success {};

  [[nodiscard]] auto handle_begin() -> std::expected<Success, AppError> {
    transactions_.emplace_back();
    return Success{};
  }

  [[nodiscard]] auto handle_commit() -> std::expected<Success, AppError> {
    if (transactions_.empty()) {
      return std::unexpected(AppError::NoActiveTransaction);
    }

    TransactionChanges committed_tx = std::move(transactions_.back());
    transactions_.pop_back();

    if (transactions_.empty()) {
      apply_changes_to_store(main_store_, committed_tx);
    } else {
      apply_changes_to_transaction(transactions_.back(), committed_tx);
    }

    return Success{};
  }

  [[nodiscard]] auto handle_rollback() -> std::expected<Success, AppError> {
    if (transactions_.empty()) {
      return std::unexpected(AppError::NoActiveTransaction);
    }
    transactions_.pop_back();
    return Success{};
  }

  [[nodiscard]] auto handle_set(Key key, Value value)
      -> std::expected<Success, AppError> {
    auto key_str = std::string(key.value);
    auto value_str = std::string(value.value);

    if (transactions_.empty()) {
      main_store_.insert_or_assign(std::move(key_str), std::move(value_str));
    } else {
      transactions_.back().insert_or_assign(std::move(key_str),
                                            std::move(value_str));
    }
    return Success{};
  }

  [[nodiscard]] auto handle_get(std::string_view key) const
      -> std::expected<std::string, AppError> {
    return get_value(key);
  }

  [[nodiscard]] auto handle_delete(std::string_view key)
      -> std::expected<Success, AppError> {
    for (const auto &transaction : transactions_ | std::views::reverse) {
      if (const auto iterator = transaction.find(key);
          iterator != transaction.end()) {
        if (!iterator->second.has_value()) {
          return std::unexpected(AppError::KeyNotFound);
        }
        transactions_.back().insert_or_assign(std::string(key), std::nullopt);
        return Success{};
      }
    }

    if (!main_store_.contains(key)) {
      return std::unexpected(AppError::KeyNotFound);
    }

    if (transactions_.empty()) {
      main_store_.erase(std::string(key));
    } else {
      transactions_.back().insert_or_assign(std::string(key), std::nullopt);
    }
    return Success{};
  }

private:
  [[nodiscard]] auto get_value(std::string_view key) const
      -> std::expected<std::string, AppError> {
    for (const auto &transaction : transactions_ | std::views::reverse) {
      if (const auto iterator = transaction.find(key);
          iterator != transaction.end()) {
        const auto &value_opt = iterator->second;
        if (value_opt.has_value()) {
          return *value_opt;
        }
        return std::unexpected(AppError::KeyNotFound);
      }
    }

    if (const auto iterator = main_store_.find(key);
        iterator != main_store_.end()) {
      return iterator->second;
    }

    return std::unexpected(AppError::KeyNotFound);
  }

  [[nodiscard]] auto key_exists(std::string_view key) const -> bool {
    return get_value(key).has_value();
  }

  static void apply_changes_to_store(MainStore &store,
                                     const TransactionChanges &changes) {
    for (const auto &[key, value_opt] : changes) {
      if (value_opt.has_value()) {
        store.insert_or_assign(key, *value_opt);
      } else {
        store.erase(key);
      }
    }
  }

  static void apply_changes_to_transaction(TransactionChanges &parent_tx,
                                           const TransactionChanges &child_tx) {
    for (const auto &[key, value_opt] : child_tx) {
      parent_tx.insert_or_assign(key, value_opt);
    }
  }

  MainStore main_store_;
  std::vector<TransactionChanges> transactions_;
};

} // namespace stasis

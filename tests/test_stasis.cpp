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

#include <gtest/gtest.h>

#include <format>
#include <memory>
#include <random>
#include <thread>
#include <utility>

#include "stasis/stasis.hpp"

class KeyValueStoreTest : public ::testing::Test {
protected:
  stasis::KeyValueStore kv_store;
};

TEST_F(KeyValueStoreTest, SetAndGetSimpleValue) {
  ASSERT_TRUE(
      kv_store.handle_set(stasis::Key{"name"}, stasis::Value{"stasis"}));
  auto result = kv_store.handle_get("name");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), "stasis");
}

TEST_F(KeyValueStoreTest, GetNonExistentKey) {
  auto result = kv_store.handle_get("nonexistent");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), stasis::AppError::KeyNotFound);
}

TEST_F(KeyValueStoreTest, SetOverwritesValue) {
  ASSERT_TRUE(
      kv_store.handle_set(stasis::Key{"key1"}, stasis::Value{"value1"}));
  ASSERT_TRUE(
      kv_store.handle_set(stasis::Key{"key1"}, stasis::Value{"value2"}));
  auto result = kv_store.handle_get("key1");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), "value2");
}

TEST_F(KeyValueStoreTest, DeleteKey) {
  ASSERT_TRUE(
      kv_store.handle_set(stasis::Key{"key1"}, stasis::Value{"value1"}));
  ASSERT_TRUE(kv_store.handle_delete("key1"));
  auto result = kv_store.handle_get("key1");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), stasis::AppError::KeyNotFound);
}

TEST_F(KeyValueStoreTest, DeleteNonExistentKey) {
  auto result = kv_store.handle_delete("nonexistent");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), stasis::AppError::KeyNotFound);
}

TEST_F(KeyValueStoreTest, SimpleTransactionCommit) {
  ASSERT_TRUE(kv_store.handle_begin());
  ASSERT_TRUE(
      kv_store.handle_set(stasis::Key{"tx_key"}, stasis::Value{"tx_value"}));
  ASSERT_TRUE(kv_store.handle_commit());

  auto post_commit_get = kv_store.handle_get("tx_key");
  ASSERT_TRUE(post_commit_get.has_value());
  EXPECT_EQ(post_commit_get.value(), "tx_value");
}

TEST_F(KeyValueStoreTest, SimpleTransactionRollback) {
  ASSERT_TRUE(kv_store.handle_begin());
  ASSERT_TRUE(
      kv_store.handle_set(stasis::Key{"tx_key"}, stasis::Value{"tx_value"}));
  ASSERT_TRUE(kv_store.handle_rollback());

  auto result = kv_store.handle_get("tx_key");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), stasis::AppError::KeyNotFound);
}

TEST_F(KeyValueStoreTest, GetWithinTransaction) {
  ASSERT_TRUE(kv_store.handle_begin());
  ASSERT_TRUE(
      kv_store.handle_set(stasis::Key{"tx_key"}, stasis::Value{"tx_value"}));

  auto result = kv_store.handle_get("tx_key");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), "tx_value");
}

TEST_F(KeyValueStoreTest, NestedTransactionCommit) {
  ASSERT_TRUE(kv_store.handle_set(stasis::Key{"outer"}, stasis::Value{"v1"}));

  ASSERT_TRUE(kv_store.handle_begin());
  ASSERT_TRUE(kv_store.handle_set(stasis::Key{"outer"}, stasis::Value{"v2"}));
  ASSERT_TRUE(kv_store.handle_set(stasis::Key{"inner"}, stasis::Value{"v3"}));

  ASSERT_TRUE(kv_store.handle_begin());
  ASSERT_TRUE(kv_store.handle_set(stasis::Key{"inner"}, stasis::Value{"v4"}));

  auto inner_val = kv_store.handle_get("inner");
  ASSERT_TRUE(inner_val.has_value());
  EXPECT_EQ(inner_val.value(), "v4");

  ASSERT_TRUE(kv_store.handle_commit());

  auto outer_val = kv_store.handle_get("inner");
  ASSERT_TRUE(outer_val.has_value());
  EXPECT_EQ(outer_val.value(), "v4");

  auto outer_key_val = kv_store.handle_get("outer");
  ASSERT_TRUE(outer_key_val.has_value());
  EXPECT_EQ(outer_key_val.value(), "v2");

  ASSERT_TRUE(kv_store.handle_commit());

  auto final_inner = kv_store.handle_get("inner");
  ASSERT_TRUE(final_inner.has_value());
  EXPECT_EQ(final_inner.value(), "v4");

  auto final_outer = kv_store.handle_get("outer");
  ASSERT_TRUE(final_outer.has_value());
  EXPECT_EQ(final_outer.value(), "v2");
}

TEST_F(KeyValueStoreTest, DeleteWithinTransaction) {
  ASSERT_TRUE(
      kv_store.handle_set(stasis::Key{"key1"}, stasis::Value{"value1"}));
  ASSERT_TRUE(kv_store.handle_begin());
  ASSERT_TRUE(kv_store.handle_delete("key1"));

  auto result = kv_store.handle_get("key1");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), stasis::AppError::KeyNotFound);

  ASSERT_TRUE(kv_store.handle_commit());

  auto final_result = kv_store.handle_get("key1");
  ASSERT_FALSE(final_result.has_value());
  EXPECT_EQ(final_result.error(), stasis::AppError::KeyNotFound);
}

TEST_F(KeyValueStoreTest, ErrorsOnNoActiveTransaction) {
  auto commit_result = kv_store.handle_commit();
  ASSERT_FALSE(commit_result.has_value());
  EXPECT_EQ(commit_result.error(), stasis::AppError::NoActiveTransaction);

  auto rollback_result = kv_store.handle_rollback();
  ASSERT_FALSE(rollback_result.has_value());
  EXPECT_EQ(rollback_result.error(), stasis::AppError::NoActiveTransaction);
}

TEST_F(KeyValueStoreTest, ConcurrentSetAndGet) {
  auto store = std::make_shared<stasis::KeyValueStore>();
  constexpr int num_threads = 50;
  std::vector<std::thread> threads;
  threads.reserve(num_threads);

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([store, i]() {
      const auto key = std::format("key{}", i);
      const auto value = std::format("value{}", i);
      auto result = store->handle_set({key}, {value});
      ASSERT_TRUE(result.has_value());
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  for (int i = 0; i < num_threads; ++i) {
    const auto key = std::format("key{}", i);
    const auto expected_value = std::format("value{}", i);
    auto result = store->handle_get(key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, expected_value);
  }
}

TEST_F(KeyValueStoreTest, ConcurrentReadWriteDeleteStressTest) {
  auto store = std::make_shared<stasis::KeyValueStore>();
  constexpr int num_keys = 100;
  constexpr int num_threads = 10;
  constexpr int operations_per_thread = 1000;

  for (int i = 0; i < num_keys; ++i) {
    const auto key = std::format("key{}", i);
    const auto value = std::format("initial_value{}", i);
    ASSERT_TRUE(store->handle_set({key}, {value}));
  }

  std::vector<std::thread> threads;
  threads.reserve(num_threads);

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([store, i]() {
      std::mt19937 gen(std::random_device{}() + i);
      std::uniform_int_distribution<> key_dist(0, num_keys - 1);
      std::uniform_int_distribution<> op_dist(0, 2);

      for (int j = 0; j < operations_per_thread; ++j) {
        const auto key = std::format("key{}", key_dist(gen));
        const int operation = op_dist(gen);

        switch (operation) {
        case 0: {
          std::ignore = store->handle_get(key);
          break;
        }
        case 1: {
          const auto value = std::format("value_thread_{}", i);
          auto result = store->handle_set({key}, {value});
          ASSERT_TRUE(result.has_value());
          break;
        }
        case 2: {
          auto result = store->handle_delete(key);
          ASSERT_TRUE(result.has_value() ||
                      result.error() == stasis::AppError::KeyNotFound);
          break;
        }
        default:
          std::unreachable();
        }
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  for (int i = 0; i < num_keys; ++i) {
    const auto key = std::format("key{}", i);
    std::ignore = store->handle_get(key);
  }
}

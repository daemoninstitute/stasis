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

#include "stasis/stasis.hpp"
#include <gtest/gtest.h>

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

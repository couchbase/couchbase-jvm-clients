/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;

/**
 * Describes how the outer document store semantics on subdoc should act.
 */
public enum StoreSemantics {
  /**
   * Replace the document, fail if it does not exist. This is the default.
   */
  REPLACE,
  /**
   * Replace the document or create it if it does not exist.
   */
  UPSERT,
  /**
   * Create the document, fail if it exists.
   */
  INSERT,

  /**
   * Convert from a tombstone to a document.
   */
  @Stability.Internal
  REVIVE
}

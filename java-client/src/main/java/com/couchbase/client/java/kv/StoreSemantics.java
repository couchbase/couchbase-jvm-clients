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
import com.couchbase.client.core.api.kv.CoreStoreSemantics;

import static java.util.Objects.requireNonNull;

/**
 * Describes how the outer document store semantics on subdoc should act.
 */
public enum StoreSemantics {
  /**
   * Replace the existing document, or fail if it does not exist.
   * This is the default.
   */
  REPLACE(CoreStoreSemantics.REPLACE),

  /**
   * Replace the document, or create it if it does not exist.
   */
  UPSERT(CoreStoreSemantics.UPSERT),

  /**
   * Create the document, or fail if it exists.
   */
  INSERT(CoreStoreSemantics.INSERT),

  /**
   * Convert a tombstone into a document.
   */
  @Stability.Internal
  REVIVE(CoreStoreSemantics.REVIVE);

  private final CoreStoreSemantics core;

  StoreSemantics(CoreStoreSemantics core) {
    this.core = requireNonNull(core);
  }

  @Stability.Internal
  public CoreStoreSemantics toCore() {
    return core;
  }
}

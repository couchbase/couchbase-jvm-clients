/*
 * Copyright (c) 2024 Couchbase, Inc.
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
package com.couchbase.client.java.search.vector;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.vector.CoreVectorQueryCombination;

/**
 * Controls how multiple vector queries are combined.
 */
public enum VectorQueryCombination {
  /**
   * All vector queries must match a document for it to be included.
   */
  AND,

  /**
   * Any vector query may match a document for it to be included.
   */
  OR;

  @Stability.Internal
  public CoreVectorQueryCombination toCore() {
    if (this == AND) {
      return CoreVectorQueryCombination.AND;
    }
    return CoreVectorQueryCombination.OR;
  }
}

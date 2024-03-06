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
import com.couchbase.client.core.api.search.vector.CoreVectorSearchOptions;
import reactor.util.annotation.Nullable;

/**
 * Options related to executing a {@link VectorSearch}.
 */
@Stability.Uncommitted
public class VectorSearchOptions {
  private @Nullable VectorQueryCombination vectorQueryCombination;

  private VectorSearchOptions() {}

  public static VectorSearchOptions vectorSearchOptions() {
    return new VectorSearchOptions();
  }

  /**
   * Specifies how multiple {@link VectorQuery}s in a {@link VectorSearch} are combined.
   *
   * @return this, for chaining.
   */
  public VectorSearchOptions vectorQueryCombination(VectorQueryCombination vectorQueryCombination) {
    this.vectorQueryCombination = vectorQueryCombination;
    return this;
  }

  @Stability.Internal
  public CoreVectorSearchOptions build() {
    return new CoreVectorSearchOptions(vectorQueryCombination == null ? null : vectorQueryCombination.toCore());
  }
}

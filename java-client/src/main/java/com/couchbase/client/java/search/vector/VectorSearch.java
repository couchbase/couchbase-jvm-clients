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
import com.couchbase.client.core.api.search.vector.CoreVectorSearch;
import com.couchbase.client.core.util.CbCollections;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * A VectorSearch allows one or more individual {@link VectorQuery}s to be executed.
 */
@Stability.Volatile
public class VectorSearch {

  private final List<VectorQuery> vectorQueries;
  private final @Nullable VectorSearchOptions vectorSearchOptions;

  private VectorSearch(List<VectorQuery> vectorQueries, @Nullable VectorSearchOptions vectorSearchOptions) {
    notNull(vectorQueries, "VectorQueries");
    this.vectorQueries = vectorQueries;
    this.vectorSearchOptions = vectorSearchOptions;
  }

  /**
   * Will execute all of the provided `vectorQueries`, using default options.
   */
  public static VectorSearch create(List<VectorQuery> vectorQueries) {
    return new VectorSearch(vectorQueries, null);
  }

  /**
   * Will execute all of the provided `vectorQueries`, using the specified options.
   */
  public static VectorSearch create(List<VectorQuery> vectorQueries, VectorSearchOptions vectorSearchOptions) {
    return new VectorSearch(vectorQueries, vectorSearchOptions);
  }

  /**
   * Will execute a single `vectorQuery`, using default options.
   */
  public static VectorSearch create(VectorQuery vectorQuery) {
    return new VectorSearch(CbCollections.listOf(vectorQuery), null);
  }

  @Stability.Internal
  public CoreVectorSearch toCore() {
    return new CoreVectorSearch(vectorQueries.stream().map(VectorQuery::toCore).collect(Collectors.toList()),
      vectorSearchOptions == null ? null : vectorSearchOptions.build());
  }
}

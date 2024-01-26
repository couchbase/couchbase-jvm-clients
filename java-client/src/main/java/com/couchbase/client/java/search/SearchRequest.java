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
package com.couchbase.client.java.search;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.queries.CoreSearchRequest;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.java.search.queries.MatchNoneQuery;
import com.couchbase.client.java.search.vector.VectorSearch;
import reactor.util.annotation.Nullable;

/**
 * A SearchRequest is used to perform operations against the Full Text Search (FTS) Couchbase service.
 * <p>
 * It can be used to send an FTS {@link SearchQuery}, and/or a {@link VectorSearch}.
 * <p>
 * If both are provided, the FTS service will merge the results.
 */
@Stability.Volatile
public class SearchRequest {

  private @Nullable SearchQuery searchQuery;
  private @Nullable VectorSearch vectorSearch;

  private SearchRequest(@Nullable SearchQuery searchQuery,
                        @Nullable VectorSearch vectorSearch) {
    this.searchQuery = searchQuery;
    this.vectorSearch = vectorSearch;
  }

  /**
   * Will run a {@link VectorSearch}.
   */
  public static SearchRequest create(VectorSearch vectorSearch) {
    return new SearchRequest(null, vectorSearch);
  }

  /**
   * Will run an FTS {@link SearchQuery}.
   */
  public static SearchRequest create(SearchQuery searchQuery) {
    return new SearchRequest(searchQuery, null);
  }

  /**
   * Can be used to run a {@link SearchQuery} together with an existing {@link VectorSearch}.
   * <p>
   * Note that a maximum of one SearchQuery and one VectorSearch can be provided.
   *
   * @return this, for chaining purposes.
   */
  public SearchRequest searchQuery(SearchQuery searchQuery) {
    if (this.searchQuery != null) {
      throw new InvalidArgumentException("A SearchQuery has already been specified.  Note that a ConjunctionQuery or DisjunctionQuery can be used to combine multiple SearchQuery objects.", null, null);
    }
    this.searchQuery = searchQuery;
    return this;
  }

  /**
   * Can be used to run a {@link VectorSearch} together with an existing {@link SearchQuery}.
   * <p>
   * Note that a maximum of one SearchQuery and one VectorSearch can be provided.
   *
   * @return this, for chaining purposes.
   */
  public SearchRequest vectorSearch(VectorSearch vectorSearch) {
    if (this.vectorSearch != null) {
      throw new InvalidArgumentException("A VectorSearch has already been specified.  Note that a single VectorSearch can take multiple VectorQuery objects, allowing multiple vector queries to be run.", null, null);
    }
    this.vectorSearch = vectorSearch;
    return this;
  }

  @Stability.Internal
  public CoreSearchRequest toCore() {
    return new CoreSearchRequest(
      searchQuery == null ? null : searchQuery.toCore(),
      vectorSearch == null ? null : vectorSearch.toCore()
    );
  }
}

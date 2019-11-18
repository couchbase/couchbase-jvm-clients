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

package com.couchbase.client.java.search.result;

import com.couchbase.client.java.search.SearchMetaData;

import java.util.List;
import java.util.Map;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

/**
 * The result of an search query, including hits and associated metadata.
 *
 * @since 3.0
 */
public class SearchResult {

    /**
     * Stores the encoded rows from the search response.
     */
    private final List<SearchRow> rows;

    /**
     * The metadata for this search result.
     */
    private final SearchMetaData meta;

    /**
     * Holds the facets if any returned.
     */
    private final Map<String, SearchFacetResult> facets;

    /**
     * Creates a new SearchResult.
     *
     * @param rows the rows/hits for this result.
     * @param meta the search metadata.
     */
    public SearchResult(final List<SearchRow> rows, final Map<String, SearchFacetResult> facets, final SearchMetaData meta) {
        this.rows = rows;
        this.facets = facets;
        this.meta = meta;
    }

    /**
     * Returns the {@link SearchMetaData} giving access to the additional metadata associated with this search
     * query.
     */
    public SearchMetaData metaData() {
        return meta;
    }

    /**
     * Returns all search hits.
     */
    public List<SearchRow> rows() {
        return rows;
    }

    /**
     * Returns the facets if present in this query.
     */
    public Map<String, SearchFacetResult> facets() {
        return facets;
    }

    @Override
    public String toString() {
        return "SearchResult{" +
          "rows=" + redactUser(rows) +
          ", meta=" + redactMeta(meta) +
          ", facets=" + redactUser(facets) +
          '}';
    }
}

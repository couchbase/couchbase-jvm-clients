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

import com.couchbase.client.core.api.search.result.CoreSearchResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.search.SearchMetaData;
import com.couchbase.client.java.search.util.SearchFacetUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The result of an search query, including hits and associated metadata.
 *
 * @since 3.0
 */
public class SearchResult {

    private final CoreSearchResult internal;
    private final JsonSerializer serializer;

    /**
     * Creates a new SearchResult.
     */
    public SearchResult(final CoreSearchResult internal, final JsonSerializer serializer) {
        this.internal = internal;
        this.serializer = serializer;
    }

    /**
     * Returns the {@link SearchMetaData} giving access to the additional metadata associated with this search
     * query.
     */
    public SearchMetaData metaData() {
        return new SearchMetaData(internal.metaData());
    }

    /**
     * Returns all search hits.
     */
    public List<SearchRow> rows() {
        return internal.rows()
                .stream()
                .map(row -> new SearchRow(row, serializer))
                .collect(Collectors.toList());
    }

    /**
     * Returns the facets if present in this query.
     */
    public Map<String, SearchFacetResult> facets() {
        Map<String, SearchFacetResult> out = new HashMap<>();
        internal.facets().forEach((k, v) -> out.put(k, SearchFacetUtil.convert(v)));
        return out;
    }

    @Override
    public String toString() {
        return internal.toString();
    }
}

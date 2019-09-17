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
     * If errors happened during the responses, they will be stored in here.
     */
    private final List<RuntimeException> errors;

    /**
     * The metadata for this search result.
     */
    private final SearchMetaData meta;

    /**
     * Creates a new SearchResult.
     *
     * @param rows the rows/hits for this result.
     * @param errors any errors that came up.
     * @param meta the search metadata.
     */
    public SearchResult(final List<SearchRow> rows, final List<RuntimeException> errors, final SearchMetaData meta) {
        this.rows = rows;
        this.errors = errors;
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
     * Returns all errors that appeared as part of the response.
     */
    public List<RuntimeException> errors() {
        return errors;
    }

    @Override
    public String toString() {
        return "SearchResult{" +
          "rows=" + rows +
          ", errors=" + errors +
          ", meta=" + meta +
          '}';
    }
}

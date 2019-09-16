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

package com.couchbase.client.java.search;

import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.result.SearchMetrics;
import com.couchbase.client.java.search.result.SearchStatus;

public class SearchMetaData {
    private final SearchStatus status;
    private final SearchMetrics metrics;

    SearchMetaData(final SearchStatus status, final SearchMetrics metrics) {
        this.status = status;
        this.metrics = metrics;
    }

    /**
     * The {@link SearchStatus} allows to check if the request was a full success ({@link SearchStatus#isSuccess()})
     * and gives more details about status for each queried index.
     */
    public SearchStatus status() {
        return status;
    }

    /**
     * If one or more facet were requested via the {@link SearchQuery#addFacet(String, SearchFacet)} method
     * when querying, contains the result of each facet.
     *
     * <p>The map keys are the names given to each requested facet.</p>
     */
    // TODO facets
//  Map<String, FacetResult> facets() {
//
//  }

    /**
     * Provides a {@link SearchMetrics} object giving statistics on the request like number of rows, total time taken...
     */
    public SearchMetrics metrics() {
        return metrics;
    }

    @Override
    public String toString() {
        return "SearchMeta{" +
          "status=" + status +
          ", metrics=" + metrics +
          '}';
    }
}

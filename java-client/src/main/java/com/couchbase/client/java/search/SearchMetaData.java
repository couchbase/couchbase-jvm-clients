package com.couchbase.client.java.search;

import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.result.SearchMetrics;
import com.couchbase.client.java.search.result.SearchStatus;

public class SearchMetaData {
    private final SearchStatus status;
    private final SearchMetrics metrics;

    public SearchMetaData(SearchStatus status,
                          SearchMetrics metrics) {
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

/*
 * Copyright (c) 2017 Couchbase, Inc.
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
package com.couchbase.client.java.search.sort;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.queries.CoreGeoCoordinates;
import com.couchbase.client.core.api.search.sort.CoreSearchSort;

/**
 * Base class for all FTS sort options in querying.
 *
 * @author Michael Nitschinger
 * @since 2.4.5
 */
public abstract class SearchSort {

    protected boolean descending;

    protected SearchSort() {
        this.descending = false;
    }

    @Stability.Internal
    public abstract CoreSearchSort toCore();

    public SearchSort desc(boolean descending) {
        this.descending = descending;
        return this;
    }

    /**
     * Sort by the document identifier.
     */
    public static SearchSortId byId() {
        return new SearchSortId();
    }

    /**
     * Sort by the hit score.
     */
    public static SearchSortScore byScore() {
        return new SearchSortScore();
    }

    /**
     * Sort by a field in the rows.
     *
     * @param field the field name.
     */
    public static SearchSortField byField(String field) {
        return new SearchSortField(field);
    }

    /**
     * Sort by distance from a geographic location.
     *
     * @param locationLon longitude of the location.
     * @param locationLat latitude of the location.
     * @param field the field name.
     */
    public static SearchSortGeoDistance byGeoDistance(double locationLon, double locationLat, String field) {
        return new SearchSortGeoDistance(CoreGeoCoordinates.lon(locationLon).lat(locationLat), field);
    }

}

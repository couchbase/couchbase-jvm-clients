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

import com.couchbase.client.core.api.search.sort.CoreSearchGeoDistanceUnits;
import com.couchbase.client.core.api.search.sort.CoreSearchSort;
import com.couchbase.client.core.api.search.sort.CoreSearchSortGeoDistance;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

/**
 * Sort by a location and unit in the rows.
 *
 * @author Michael Nitschinger
 * @since 2.4.5
 */
public class SearchSortGeoDistance extends SearchSort {

    private final String field;
    private final double locationLon;
    private final double locationLat;
    private CoreSearchGeoDistanceUnits unit;

    SearchSortGeoDistance(double locationLon, double locationLat, String field) {
        this.field = field;
        this.locationLon = locationLon;
        this.locationLat = locationLat;
    }

    @Override
    public SearchSortGeoDistance desc(final boolean descending) {
        super.desc(descending);
        return this;
    }

    public SearchSortGeoDistance unit(final SearchGeoDistanceUnits unit) {
        this.unit = unit.toCore();
        return this;
    }

    @Override
    public CoreSearchSort toCore() {
        return new CoreSearchSortGeoDistance(locationLon, locationLat, field, unit, descending);
    }
}

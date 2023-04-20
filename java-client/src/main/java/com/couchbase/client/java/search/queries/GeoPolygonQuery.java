/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.java.search.queries;

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.api.search.queries.CoreGeoCoordinates;
import com.couchbase.client.core.api.search.queries.CoreGeoPolygonQuery;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.util.Coordinate;

import java.util.List;

import static com.couchbase.client.core.util.CbCollections.transform;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * A search query which allows to match inside a geo polygon.
 */
@SinceCouchbase("6.5.1")
public class GeoPolygonQuery extends SearchQuery {

    private final List<Coordinate> coordinates;

    private String field;

    public GeoPolygonQuery(final List<Coordinate> coordinates) {
        this.coordinates = notNullOrEmpty(coordinates, "GeoPolygonQuery Coordinates");
    }

    /**
     * Allows to specify which field the query should apply to (default is null).
     *
     * @param field the name of the field in the index/document (if null it is not considered).
     * @return this {@link GeoPolygonQuery} for chaining purposes.
     */
    public GeoPolygonQuery field(final String field) {
        this.field = field;
        return this;
    }

    @Override
    public GeoPolygonQuery boost(final double boost) {
        super.boost(boost);
        return this;
    }

    @Override
    public CoreSearchQuery toCore() {
        return new CoreGeoPolygonQuery(
                transform(coordinates, it -> CoreGeoCoordinates.lat(it.lat()).lon(it.lon())),
                field,
                boost);
    }
}

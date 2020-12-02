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

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.util.Coordinate;
import com.couchbase.client.java.search.SearchQuery;

import java.util.List;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * A search query which allows to match inside a geo polygon.
 */
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
    protected void injectParams(final JsonObject input) {
        final JsonArray points = JsonArray.create();

        for (Coordinate coordinate : coordinates) {
            points.add(JsonArray.from(coordinate.lon(), coordinate.lat()));
        }
        input.put("polygon_points", points);

        if (field != null) {
            input.put("field", field);
        }
    }
}

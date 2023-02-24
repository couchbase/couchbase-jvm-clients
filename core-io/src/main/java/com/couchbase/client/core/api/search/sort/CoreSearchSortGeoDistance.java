/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.api.search.sort;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Internal
public class CoreSearchSortGeoDistance extends CoreSearchSort {

    private final String field;
    private final double locationLon;
    private final double locationLat;
    private final @Nullable CoreSearchGeoDistanceUnits unit;

    public CoreSearchSortGeoDistance(double locationLon,
                                     double locationLat,
                                     String field,
                                     @Nullable CoreSearchGeoDistanceUnits unit,
                                     boolean descending) {
        super(descending);
        this.field = notNull(field, "Field");
        this.locationLon = locationLon;
        this.locationLat = locationLat;
        this.unit = unit;
    }

    @Override
    protected String identifier() {
        return "geo_distance";
    }

    @Override
    public void injectParams(ObjectNode queryJson) {
        super.injectParams(queryJson);
        ArrayNode location = Mapper.createArrayNode();
        location.add(locationLon);
        location.add(locationLat);
        queryJson.set("location", location);
        queryJson.put("field", field);
        if (unit != null) {
            queryJson.put("unit", unit.identifier());
        }
    }
}

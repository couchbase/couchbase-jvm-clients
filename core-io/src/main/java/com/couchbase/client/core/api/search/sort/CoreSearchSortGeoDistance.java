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
import com.couchbase.client.core.api.search.queries.CoreGeoPoint;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.protostellar.search.v1.GeoDistanceSorting;
import com.couchbase.client.protostellar.search.v1.Sorting;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.toLatLng;
import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Internal
public class CoreSearchSortGeoDistance extends CoreSearchSort {

    private final String field;
    private final CoreGeoPoint location;
    private final @Nullable CoreSearchGeoDistanceUnits unit;

    public CoreSearchSortGeoDistance(CoreGeoPoint location,
                                     String field,
                                     @Nullable CoreSearchGeoDistanceUnits unit,
                                     boolean descending) {
        super(descending);
        this.field = notNull(field, "Field");
        this.location = notNull(location, "Location");
        this.unit = unit;
    }

    @Override
    protected String identifier() {
        return "geo_distance";
    }

    @Override
    protected void injectParams(ObjectNode queryJson) {
        super.injectParams(queryJson);
        queryJson.set("location", location.toJson());
        queryJson.put("field", field);
        if (unit != null) {
            queryJson.put("unit", unit.identifier());
        }
    }

    @Override
    public Sorting asProtostellar() {
        GeoDistanceSorting.Builder builder = GeoDistanceSorting.newBuilder()
                .setField(field)
                .setDescending(descending)
                .setCenter(toLatLng(location));

        if (unit != null) {
            builder.setUnit(unit.identifier());
        }

        return Sorting.newBuilder().setGeoDistanceSorting(builder).build();
    }
}

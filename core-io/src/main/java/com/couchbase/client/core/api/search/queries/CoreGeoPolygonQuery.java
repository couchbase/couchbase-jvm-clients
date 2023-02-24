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

package com.couchbase.client.core.api.search.queries;

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.protostellar.search.v1.GeoPolygonQuery;
import com.couchbase.client.protostellar.search.v1.LatLng;
import com.couchbase.client.protostellar.search.v1.Query;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

@SinceCouchbase("6.5.1")
@Stability.Internal
public class CoreGeoPolygonQuery extends CoreSearchQuery {

    private final List<CoreCoordinate> coordinates;
    private final @Nullable String field;

    public CoreGeoPolygonQuery(final List<CoreCoordinate> coordinates,
                               @Nullable String field,
                               @Nullable Double boost) {
      super(boost);
        this.coordinates = notNullOrEmpty(coordinates, "GeoPolygonQuery Coordinates");
        this.field = field;
    }

    @Override
    protected void injectParams(final ObjectNode input) {
        ArrayNode points = Mapper.createArrayNode();
        coordinates.forEach(c -> {
          ArrayNode coords = Mapper.createArrayNode();
          coords.add(c.lon());
          coords.add(c.lat());
          points.add(coords);
        });

        input.set("polygon_points", points);

        if (field != null) {
            input.put("field", field);
        }
    }

  @Override
  public Query asProtostellar() {
    GeoPolygonQuery.Builder builder = GeoPolygonQuery.newBuilder()
            .addAllVertices(coordinates.stream()
                    .map(c -> LatLng.newBuilder().setLongitude(c.lon()).setLatitude(c.lat()).build())
                    .collect(Collectors.toList()));

    if (field != null) {
      builder.setField(field);
    }

    if (boost != null) {
      builder.setBoost(boost.floatValue());
    }

    return Query.newBuilder().setGeoPolygonQuery(builder).build();

  }
}

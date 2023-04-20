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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.protostellar.search.v1.GeoDistanceQuery;
import com.couchbase.client.protostellar.search.v1.LatLng;
import com.couchbase.client.protostellar.search.v1.Query;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.requireCoordinates;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreGeoDistanceQuery extends CoreSearchQuery {

  private final CoreGeoPoint location;
  private final String distance;
  private final @Nullable String field;

  public CoreGeoDistanceQuery(double locationLon, double locationLat, String distance, @Nullable String field, @Nullable Double boost) {
    this(
        CoreGeoCoordinates.lon(locationLon).lat(locationLat),
        distance,
        field,
        boost
    );
  }

  public CoreGeoDistanceQuery(CoreGeoPoint location, String distance, @Nullable String field, @Nullable Double boost) {
    super(boost);
    this.location = requireNonNull(location);
    this.distance = notNullOrEmpty(distance, "Distance");
    this.field = field;
  }

  @Override
  protected void injectParams(ObjectNode input) {
    input.set("location", location.toJson());
    input.put("distance", distance);
    if (field != null) {
      input.put("field", field);
    }
  }

  @Override
  public Query asProtostellar() {
    CoreGeoCoordinates location = requireCoordinates(this.location);

    GeoDistanceQuery.Builder builder = GeoDistanceQuery.newBuilder()
        .setCenter(LatLng.newBuilder().setLongitude(location.lon()).setLatitude(location.lat()))
        .setDistance(distance);

    if (field != null) {
      builder.setField(field);
    }

    if (boost != null) {
      builder.setBoost(boost.floatValue());
    }

    return Query.newBuilder().setGeoDistanceQuery(builder).build();
  }
}

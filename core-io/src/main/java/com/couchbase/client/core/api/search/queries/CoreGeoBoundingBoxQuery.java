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
import com.couchbase.client.protostellar.search.v1.GeoBoundingBoxQuery;
import com.couchbase.client.protostellar.search.v1.LatLng;
import com.couchbase.client.protostellar.search.v1.Query;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.requireCoordinates;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreGeoBoundingBoxQuery extends CoreSearchQuery {

  private final CoreGeoPoint topLeft;
  private final CoreGeoPoint bottomRight;
  private @Nullable final String field;

  public CoreGeoBoundingBoxQuery(double topLeftLon,
                                 double topLeftLat,
                                 double bottomRightLon,
                                 double bottomRightLat,
                                 @Nullable String field,
                                 @Nullable Double boost) {
    this(
        CoreGeoCoordinates.lon(topLeftLon).lat(topLeftLat),
        CoreGeoCoordinates.lon(bottomRightLon).lat(bottomRightLat),
        field,
        boost
    );
  }

  public CoreGeoBoundingBoxQuery(
      CoreGeoPoint topLeft,
      CoreGeoPoint bottomRight,
      @Nullable String field,
      @Nullable Double boost
  ) {
    super(boost);
    this.topLeft = requireNonNull(topLeft);
    this.bottomRight = requireNonNull(bottomRight);
    this.field = field;
  }

  @Override
  protected void injectParams(ObjectNode input) {
    input.set("top_left", topLeft.toJson());
    input.set("bottom_right", bottomRight.toJson());
    if (field != null) {
      input.put("field", field);
    }
  }

  @Override
  public Query asProtostellar() {
    CoreGeoCoordinates topLeft = requireCoordinates(this.topLeft);
    CoreGeoCoordinates bottomRight = requireCoordinates(this.bottomRight);

    GeoBoundingBoxQuery.Builder builder = GeoBoundingBoxQuery.newBuilder()
            .setTopLeft(LatLng.newBuilder().setLongitude(topLeft.lon()).setLatitude(topLeft.lat()))
            .setBottomRight(LatLng.newBuilder().setLongitude(bottomRight.lon()).setLatitude(bottomRight.lat()));

    if (field != null) {
      builder.setField(field);
    }

    if (boost != null) {
      builder.setBoost(boost.floatValue());
    }

    return Query.newBuilder().setGeoBoundingBoxQuery(builder).build();
  }
}

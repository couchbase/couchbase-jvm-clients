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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.protostellar.search.v1.GeoBoundingBoxQuery;
import com.couchbase.client.protostellar.search.v1.LatLng;
import com.couchbase.client.protostellar.search.v1.Query;
import reactor.util.annotation.Nullable;

@Stability.Internal
public class CoreGeoBoundingBoxQuery extends CoreSearchQuery {

  private final double topLeftLat;
  private final double topLeftLon;
  private final double bottomRightLat;
  private final double bottomRightLon;
  private @Nullable final String field;

  public CoreGeoBoundingBoxQuery(double topLeftLon,
                                 double topLeftLat,
                                 double bottomRightLon,
                                 double bottomRightLat,
                                 @Nullable String field,
                                 @Nullable Double boost) {
    super(boost);
    this.topLeftLat = topLeftLat;
    this.topLeftLon = topLeftLon;
    this.bottomRightLat = bottomRightLat;
    this.bottomRightLon = bottomRightLon;
    this.field = field;
  }

  @Override
  protected void injectParams(ObjectNode input) {
    ArrayNode topLeft = Mapper.createArrayNode();
    topLeft.add(topLeftLon);
    topLeft.add(topLeftLat);
    ArrayNode bottomRight = Mapper.createArrayNode();
    bottomRight.add(bottomRightLon);
    bottomRight.add(bottomRightLat);
    input.set("top_left", topLeft);
    input.set("bottom_right", bottomRight);
    if (field != null) {
      input.put("field", field);
    }
  }

  @Override
  public Query asProtostellar() {
    GeoBoundingBoxQuery.Builder builder = GeoBoundingBoxQuery.newBuilder()
            .setTopLeft(LatLng.newBuilder().setLongitude(topLeftLon).setLatitude(topLeftLat))
            .setBottomRight(LatLng.newBuilder().setLongitude(bottomRightLon).setLatitude(bottomRightLat));

    if (field != null) {
      builder.setField(field);
    }

    if (boost != null) {
      builder.setBoost(boost.floatValue());
    }

    return Query.newBuilder().setGeoBoundingBoxQuery(builder).build();
  }
}

/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.api.search.queries;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.json.Mapper;

/**
 * To create a new instance:
 * <pre>
 * CoreGeoCoordinates eiffelTower = CoreGeoCoordinates.lat(48.858093).lon(2.294694);
 * </pre>
 * Or, if you prefer to specify longitude first:
 * <pre>
 * CoreGeoCoordinates eiffelTower = CoreGeoCoordinates.lon(2.294694).lat(48.858093);
 * </pre>
 */
@Stability.Internal
public class CoreGeoCoordinates implements CoreGeoPoint {
  private final double lat;
  private final double lon;

  private CoreGeoCoordinates(double lat, double lon) {
    this.lat = lat;
    this.lon = lon;
  }

  public double lat() {
    return lat;
  }

  public double lon() {
    return lon;
  }

  @Override
  public JsonNode toJson() {
    ArrayNode array = Mapper.createArrayNode();
    array.add(lon);
    array.add(lat);
    return array;
  }

  @Override
  public String toString() {
    return "CoreGeoCoordinates{" +
        "lat=" + lat +
        ", lon=" + lon +
        '}';
  }

  public static Lat lat(double lat) {
    return new Lat(lat);
  }

  public static Lon lon(double lon) {
    return new Lon(lon);
  }


  /**
   * A staged builder that holds a coordinate's latitude value.
   * Call {@link #lon} to finish specifying the coordinate.
   */
  public static final class Lat {
    private final double lat;

    private Lat(double lat) {
      this.lat = lat;
    }

    public CoreGeoCoordinates lon(double lon) {
      return new CoreGeoCoordinates(lat, lon);
    }
  }

  /**
   * A staged builder that holds a coordinate's longitude value.
   * Call {@link #lat} to finish specifying the coordinate.
   */
  public static final class Lon {
    private final double lon;

    private Lon(double lon) {
      this.lon = lon;
    }

    public CoreGeoCoordinates lat(double lat) {
      return new CoreGeoCoordinates(lat, lon);
    }
  }
}

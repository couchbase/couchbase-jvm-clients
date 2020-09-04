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

package com.couchbase.client.java.util;

/**
 * A coordinate is a tuple of a latitude and a longitude.
 * <p>
 * If you need to construct a coordinate, use the {@link #ofLonLat(double, double)} static method.
 */
public class Coordinate {

  private final double lon;
  private final double lat;

  /**
   * Creates a new {@link Coordinate} with a longitude and a latitude.
   *
   * @param lon the longitude of the coordinate.
   * @param lat the latitude of the coordinate.
   * @return a new {@link Coordinate}.
   */
  public static Coordinate ofLonLat(double lon, double lat) {
    return new Coordinate(lon, lat);
  }

  private Coordinate(final double lon, final double lat) {
    this.lon = lon;
    this.lat = lat;
  }

  /**
   * Returns the longitude of this coordinate.
   */
  public double lon() {
    return lon;
  }

  /**
   * Returns the latitude of this coordinate.
   */
  public double lat() {
    return lat;
  }

}
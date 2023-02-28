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

import com.couchbase.client.core.annotation.Stability;

/**
 * A coordinate is a tuple of a latitude and a longitude.
 * <p>
 * To create a new instance:
 * <pre>
 * Coordinate eiffelTower = Coordinate.lat(48.858093).lon(2.294694);
 * </pre>
 * Or, if you prefer to specify longitude first:
 * <pre>
 * Coordinate eiffelTower = Coordinate.lon(2.294694).lat(48.858093);
 * </pre>
 */
public class Coordinate {

  private final double lon;
  private final double lat;

  /**
   * Creates a new {@link Coordinate} with a longitude and a latitude.
   * <p>
   * To avoid confusing the order of the longitude and latitude parameters,
   * please use {@link #lat(double)} or {@link #lon(double)} instead.
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

  /**
   * Returns a new staged Coordinate builder, with the specified
   * latitude value.
   * <p>
   * Complete the Coordinate by calling {@link CoordinateBuilderLatitude#lon(double)}
   * on the result. Example usage:
   * <pre>
   * Coordinate eiffelTower = Coordinate.lat(48.858093).lon(2.294694);
   * </pre>
   */
  @Stability.Uncommitted
  public static CoordinateBuilderLatitude lat(double lat) {
    return new CoordinateBuilderLatitude(lat);
  }

  /**
   * Returns a new staged Coordinate builder, with the specified
   * longitude value.
   * <p>
   * Complete the Coordinate by calling {@link CoordinateBuilderLongitude#lat(double)}
   * on the result. Example usage:
   * <pre>
   * Coordinate eiffelTower = Coordinate.lon(2.294694).lat(48.858093);
   * </pre>
   */
  @Stability.Uncommitted
  public static CoordinateBuilderLongitude lon(double lon) {
    return new CoordinateBuilderLongitude(lon);
  }

  @Override
  public String toString() {
    return "Coordinate{" +
      "lon=" + lon +
      ", lat=" + lat +
      '}';
  }

  /**
   * A staged builder that holds a coordinate's latitude value.
   * Call {@link #lon} to finish specifying the coordinate.
   */
  public static final class CoordinateBuilderLatitude {
    private final double lat;

    private CoordinateBuilderLatitude(double lat) {
      this.lat = lat;
    }

    public Coordinate lon(double lon) {
      return new Coordinate(lon, lat);
    }
  }

  /**
   * A staged builder that holds a coordinate's longitude value.
   * Call {@link #lat} to finish specifying the coordinate.
   */
  public static final class CoordinateBuilderLongitude {
    private final double lon;

    private CoordinateBuilderLongitude(double lon) {
      this.lon = lon;
    }

    public Coordinate lat(double lat) {
      return new Coordinate(lon, lat);
    }
  }
}

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

package com.couchbase.client.java.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CoordinateTest {

  private static final double LON = 1;
  private static final double LAT = 2;

  private static void check(Coordinate c) {
    assertEquals(LON, c.lon());
    assertEquals(LAT, c.lat());
  }

  @Test
  void ofLonLat() {
    check(Coordinate.ofLonLat(LON, LAT));
  }

  @Test
  void stagedBuilderLonFirst() {
    check(Coordinate.lon(LON).lat(LAT));
  }

  @Test
  void stagedBuilderLatFirst() {
    check(Coordinate.lat(LAT).lon(LON));
  }
}

/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java.search.sort;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.sort.CoreSearchGeoDistanceUnits;

public enum SearchGeoDistanceUnits {
  Meters(CoreSearchGeoDistanceUnits.METERS),
  Miles(CoreSearchGeoDistanceUnits.MILES),
  Centimeters(CoreSearchGeoDistanceUnits.CENTIMETERS),
  Millimeters(CoreSearchGeoDistanceUnits.MILLIMETERS),
  NauticalMiles(CoreSearchGeoDistanceUnits.NAUTICAL_MILES),
  Kilometers(CoreSearchGeoDistanceUnits.KILOMETERS),
  Feet(CoreSearchGeoDistanceUnits.FEET),
  Yards(CoreSearchGeoDistanceUnits.YARDS),
  Inch(CoreSearchGeoDistanceUnits.INCH);

  private final CoreSearchGeoDistanceUnits internal;

  SearchGeoDistanceUnits(final CoreSearchGeoDistanceUnits identifier) {
    this.internal = identifier;
  }

  public String identifier() {
    return internal.identifier();
  }

  @Stability.Internal
  public CoreSearchGeoDistanceUnits toCore() {
    return internal;
  }
}

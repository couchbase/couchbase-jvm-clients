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

@Stability.Internal
public class CoreCoordinate {
  private final double lon;
  private final double lat;

  public CoreCoordinate(final double lon, final double lat) {
    this.lon = lon;
    this.lat = lat;
  }

  public double lon() {
    return lon;
  }

  public double lat() {
    return lat;
  }
}
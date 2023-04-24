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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.TextNode;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreGeohash implements CoreGeoPoint {
  private final String value;

  public CoreGeohash(String value) {
    this.value = requireNonNull(value);
  }

  @Override
  public JsonNode toJson() {
    return new TextNode(value);
  }

  public String value() {
    return value;
  }

  @Override
  public String toString() {
    return "CoreGeohash{" +
        "value='" + value + '\'' +
        '}';
  }
}

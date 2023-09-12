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

package com.couchbase.client.core.config;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CollectionsManifestCollection {

  private final String name;
  private final String uid;
  private final Integer maxExpiry;
  private final Boolean history;

  @JsonCreator
  public CollectionsManifestCollection(
    @JsonProperty("name") String name,
    @JsonProperty("uid") String uid,
    @JsonProperty("maxTTL") Integer maxExpiry,
    @JsonProperty("history") Boolean history) {
    this.name = name;
    this.uid = uid;
    // Even if maxTTL is not set, it still appears in the JSON as 0.  Except on older server versions.
    this.maxExpiry = maxExpiry;
    // Similarly history will appear as false in the JSON if not set.  Also except on older server versions.
    this.history = history;
  }

  public String name() {
    return name;
  }

  public String uid() {
    return uid;
  }

  public Integer maxExpiry() {
    return maxExpiry;
  }

  public Boolean history() {
    return history;
  }

  @Override
  public String toString() {
    return "CollectionsManifestCollection{" +
      "name='" + name + '\'' +
      ", uid='" + uid + '\'' +
      ", maxExpiry=" + maxExpiry + '\'' +
      ", history=" + history +
      '}';
  }
}

/*
 * Copyright (c) 2017 Couchbase, Inc.
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

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CollectionsManifest {

  private final String uid;
  private final List<CollectionsManifestScope> scopes;

  @JsonCreator
  public CollectionsManifest(
    @JsonProperty("uid") String uid,
    @JsonProperty("scopes") List<CollectionsManifestScope> scopes) {
    this.uid = uid;
    this.scopes = scopes;
  }

  public String uid() {
    return uid;
  }

  public List<CollectionsManifestScope> scopes() {
    return scopes;
  }

  @Override
  public String toString() {
    return "CollectionsManifest{" +
      "uid='" + uid + '\'' +
      ", scopes=" + scopes +
      '}';
  }
}

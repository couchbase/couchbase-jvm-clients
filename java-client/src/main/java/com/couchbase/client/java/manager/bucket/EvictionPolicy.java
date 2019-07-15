/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.manager.bucket;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Locale;

@Stability.Volatile
public enum EvictionPolicy {
  @JsonProperty("valueOnly") VALUE_ONLY("valueOnly"),
  @JsonProperty("fullEviction") FULL("fullEviction");

  private final String alias;

  EvictionPolicy(String alias) {
    this.alias = alias;
  }

  public String alias() {
    return alias;
  }

}

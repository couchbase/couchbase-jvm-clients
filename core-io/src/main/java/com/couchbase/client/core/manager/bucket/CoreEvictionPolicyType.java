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

package com.couchbase.client.core.manager.bucket;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonValue;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public enum CoreEvictionPolicyType {
  FULL("fullEviction"),
  VALUE_ONLY("valueOnly"),
  NOT_RECENTLY_USED("nruEviction"),
  NO_EVICTION("noEviction");

  private final String alias;

  CoreEvictionPolicyType(String alias) {
    this.alias = requireNonNull(alias);
  }

  @JsonValue
  @Stability.Internal
  public String alias() {
    return alias;
  }
}

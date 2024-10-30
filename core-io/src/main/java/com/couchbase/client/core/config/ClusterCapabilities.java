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

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonValue;

/**
 * Contains all the cluster capabilities this SDK supports (depending on the server version, the cluster may
 * export more than these).
 *
 * @deprecated In favor of {@link com.couchbase.client.core.topology.ClusterCapability}
 */
@Deprecated
public enum ClusterCapabilities {

  ENHANCED_PREPARED_STATEMENTS("enhancedPreparedStatements"),
  SCOPED_SEARCH_INDEX("scopedSearchIndex"),
  VECTOR_SEARCH("vectorSearch");

  private final String raw;

  ClusterCapabilities(String raw) {
    this.raw = raw;
  }

  @JsonValue
  public String getRaw() {
    return raw;
  }

}

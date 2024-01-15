/*
 * Copyright (c) 2024 Couchbase, Inc.
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
package com.couchbase.client.core.api.search.vector;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import reactor.util.annotation.Nullable;

import java.util.List;

import static com.couchbase.client.core.util.Validators.notNull;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreVectorSearch {

  public final List<CoreVectorQuery> vectorQueries;
  public final @Nullable CoreVectorSearchOptions options;

  public CoreVectorSearch(List<CoreVectorQuery> vectorQueries,
                          @Nullable CoreVectorSearchOptions options) {
    if (vectorQueries.isEmpty()) {
      throw new InvalidArgumentException("At least one VectorQuery must be specified", null, null);
    }
    vectorQueries.forEach(vq -> notNull(vq, "VectorQuery"));
    this.vectorQueries = requireNonNull(vectorQueries);
    this.options = options;
  }

  public void injectTopLevel(ObjectNode toSend) {
    ArrayNode knn = Mapper.createArrayNode();
    for (CoreVectorQuery vectorQuery : vectorQueries) {
      knn.add(vectorQuery.toJson());
    }
    toSend.set("knn", knn);
    if (options != null) {
      options.injectTopLevel(toSend);
    }
  }
}

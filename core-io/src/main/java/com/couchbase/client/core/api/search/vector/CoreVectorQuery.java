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

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

@Stability.Internal
public class CoreVectorQuery {
  public static final int DEFAULT_NUM_CANDIDATES = 3;

  private final float[] vector;
  private final String field;
  private final @Nullable Integer numCandidates;
  private final @Nullable Double boost;

  public CoreVectorQuery(float[] vector,
                         String field,
                         @Nullable Integer numCandidates,
                         @Nullable Double boost) {
    this.vector = notNull(vector, "Vector");
    this.numCandidates = numCandidates;
    this.field = notNullOrEmpty(field, "Field");
    this.boost = boost;

    if (numCandidates != null && numCandidates < 1) {
      throw InvalidArgumentException.fromMessage("If numCandidates is specified it must be >= 1");
    }
    if (vector.length == 0) {
      throw InvalidArgumentException.fromMessage("A vector query must have at least one member");
    }
  }

  public ObjectNode toJson() {
    ObjectNode outer = Mapper.createObjectNode();
    ArrayNode array = Mapper.createArrayNode();

    for (float v : vector) {
      array.add(v);
    }
    outer.set("vector", array);
    outer.put("field", this.field);
    outer.put("k", numCandidates != null ? numCandidates : DEFAULT_NUM_CANDIDATES);
    if (boost != null) {
      outer.put("boost", boost);
    }
    return outer;
  }
}

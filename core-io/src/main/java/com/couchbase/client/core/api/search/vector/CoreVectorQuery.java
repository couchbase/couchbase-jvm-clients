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

import java.util.Arrays;
import java.util.Objects;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

@Stability.Internal
public class CoreVectorQuery {
  public static final int DEFAULT_NUM_CANDIDATES = 3;

  // exactly one is non-null
  @Nullable private final float[] vector;
  @Nullable private final String base64EncodedVector;

  private final String field;
  private final @Nullable Integer numCandidates;
  private final @Nullable Double boost;

  public CoreVectorQuery(
      @Nullable float[] vector,
      @Nullable String base64EncodedVector,
      String field,
      @Nullable Integer numCandidates,
      @Nullable Double boost
  ) {
    if (countNonNull(vector, base64EncodedVector) != 1) {
      throw InvalidArgumentException.fromMessage("Exactly one of `vector` or `base64EncodedVector` must be non-null.");
    }

    this.vector = vector;
    this.base64EncodedVector = base64EncodedVector;

    this.numCandidates = numCandidates;
    this.field = notNullOrEmpty(field, "Field");
    this.boost = boost;

    if (numCandidates != null && numCandidates < 1) {
      throw InvalidArgumentException.fromMessage("If numCandidates is specified, it must be >= 1, but got: " + numCandidates);
    }
    if (vector != null && vector.length == 0) {
      throw InvalidArgumentException.fromMessage("A vector must have at least one element.");
    }
  }

  private static int countNonNull(Object... objects) {
    return (int) Arrays.stream(objects).filter(Objects::nonNull).count();
  }

  public ObjectNode toJson() {
    ObjectNode outer = Mapper.createObjectNode();
    outer.put("field", this.field);
    outer.put("k", numCandidates != null ? numCandidates : DEFAULT_NUM_CANDIDATES);
    if (boost != null) {
      outer.put("boost", boost);
    }

    if (vector != null) {
      ArrayNode array = Mapper.createArrayNode();
      for (float v : vector) {
        array.add(v);
      }
      outer.set("vector", array);

    } else {
      outer.put("vector_base64", base64EncodedVector);
    }

    return outer;
  }
}

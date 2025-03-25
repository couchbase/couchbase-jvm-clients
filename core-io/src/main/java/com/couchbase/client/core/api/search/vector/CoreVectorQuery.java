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

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreVectorQuery {
  public static final int DEFAULT_NUM_CANDIDATES = 3;

  private final CoreVector vector;
  private final String field;
  private final @Nullable Integer numCandidates;
  private final @Nullable Double boost;
  private final @Nullable CoreSearchQuery prefilter;

  public CoreVectorQuery(
      CoreVector vector,
      String field,
      @Nullable Integer numCandidates,
      @Nullable Double boost,
      @SinceCouchbase("7.6.4") @Nullable CoreSearchQuery prefilter
  ) {
    this.vector = requireNonNull(vector);
    this.numCandidates = numCandidates;
    this.field = notNullOrEmpty(field, "Field");
    this.boost = boost;
    this.prefilter = prefilter;

    if (numCandidates != null && numCandidates < 1) {
      throw InvalidArgumentException.fromMessage("If numCandidates is specified, it must be >= 1, but got: " + numCandidates);
    }
  }

  public ObjectNode toJson() {
    ObjectNode outer = Mapper.createObjectNode();
    outer.put("field", this.field);
    outer.put("k", numCandidates != null ? numCandidates : DEFAULT_NUM_CANDIDATES);
    if (prefilter != null) {
      outer.set("filter", prefilter.export());
    }
    if (boost != null) {
      outer.put("boost", boost);
    }

    vector.writeTo(outer);

    return outer;
  }
}

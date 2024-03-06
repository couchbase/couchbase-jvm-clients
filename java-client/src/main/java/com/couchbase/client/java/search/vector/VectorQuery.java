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
package com.couchbase.client.java.search.vector;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.vector.CoreVectorQuery;
import reactor.util.annotation.Nullable;

@Stability.Uncommitted
public class VectorQuery {
  private final float[] vectorQuery;
  private final String vectorField;
  private @Nullable Integer numCandidates;
  private @Nullable Double boost;

  private VectorQuery(String vectorField, float[] vectorQuery) {
    this.vectorField = vectorField;
    this.vectorQuery = vectorQuery;
  }

  /**
   * @param vectorField the document field that contains the vector.
   * @param vectorQuery the vector query to run.
   * @return
   */
  public static VectorQuery create(String vectorField, float[] vectorQuery) {
    return new VectorQuery(vectorField, vectorQuery);
  }

  /**
   * This is the number of results that will be returned from this vector query.
   *
   * @return this, for chaining.
   */
  public VectorQuery numCandidates(int numCandidates) {
    this.numCandidates = numCandidates;
    return this;
  }

  /**
   * Can be used to control how much weight to give the results of this query vs other queries.
   *
   * See the <a href="https://docs.couchbase.com/server/current/fts/fts-query-string-syntax-boosting.html">FTS documentation</a> for details.
   *
   * @return this, for chaining.
   */
  public VectorQuery boost(double boost) {
    this.boost = boost;
    return this;
  }

  @Stability.Internal
  public CoreVectorQuery toCore() {
    return new CoreVectorQuery(vectorQuery, vectorField, numCandidates, boost);
  }
}

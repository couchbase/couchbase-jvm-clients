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

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.vector.CoreVector;
import com.couchbase.client.core.api.search.vector.CoreVectorQuery;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Uncommitted
public class VectorQuery {
  private final CoreVector vector;
  private final String vectorField;
  private @Nullable Integer numCandidates;
  private @Nullable Double boost;

  private VectorQuery(String vectorField, float[] vector) {
    this.vectorField = notNull(vectorField, "vectorField");
    this.vector = CoreVector.of(vector);
  }

  private VectorQuery(String vectorField, String base64EncodedVector) {
    this.vectorField = notNull(vectorField, "vectorField");
    this.vector = CoreVector.of(base64EncodedVector);
  }

  /**
   * @param vectorField the document field that contains the vector.
   * @param vector the vector to search for.
   */
  @SinceCouchbase("7.6")
  public static VectorQuery create(String vectorField, float[] vector) {
    return new VectorQuery(vectorField, vector);
  }

  /**
   * @param vectorField the document field that contains the vector.
   * @param base64EncodedVector the vector to search for, as a Base64-encoded sequence of little-endian IEEE 754 floats.
   */
  @SinceCouchbase("7.6.2")
  public static VectorQuery create(String vectorField, String base64EncodedVector) {
    return new VectorQuery(vectorField, base64EncodedVector);
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
   * <p>
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
    return new CoreVectorQuery(vector, vectorField, numCandidates, boost);
  }
}

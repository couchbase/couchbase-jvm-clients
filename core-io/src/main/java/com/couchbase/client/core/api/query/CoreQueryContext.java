/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.api.query;

import com.couchbase.client.core.annotation.Stability;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreQueryContext {
  private final String namespace;
  private final String bucket;
  private final String scope;

  private CoreQueryContext(String namespace, String bucket, String scope) {
    this.namespace = requireNonNull(namespace);
    this.bucket = requireNonNull(bucket);
    this.scope = requireNonNull(scope);

    if (namespace.contains("`") || bucket.contains("`") || scope.contains("`")) {
      throw new IllegalArgumentException("Query context components may not contain backticks");
    }
  }

  /**
   * Returns a query context for the default namespace, with the given bucket and scope names.
   */
  public static CoreQueryContext of(String bucket, String scope) {
    return new CoreQueryContext("default", bucket, scope);
  }

  public String format() {
    return namespace + ":`" + bucket + "`.`" + scope + "`";
  }

  public String bucket() {
    return bucket;
  }

  public String scope() {
    return scope;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CoreQueryContext that = (CoreQueryContext) o;
    return namespace.equals(that.namespace) && bucket.equals(that.bucket) && scope.equals(that.scope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, bucket, scope);
  }

  @Override
  public String toString() {
    return format();
  }
}

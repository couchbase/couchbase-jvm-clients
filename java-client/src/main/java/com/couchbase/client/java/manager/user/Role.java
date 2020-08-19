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

package com.couchbase.client.java.manager.user;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Stability.Volatile
@JsonIgnoreProperties(ignoreUnknown = true)
public class Role {

  private final String name;
  private final Optional<String> bucket;
  private final Optional<String> scope;
  private final Optional<String> collection;

  /**
   * Creates a new system-wide role (not specific to a bucket)
   *
   * @param roleName symbolic name of the role
   */
  public Role(String roleName) {
    this(roleName, null, null, null);
  }

  /**
   * Creates a new role. If the bucket parameter is null, a system-wide role is created.
   * Otherwise, the role applies to all scopes and collections within the bucket.
   *
   * @param roleName symbolic name of the role
   * @param bucket (nullable) target of the role, or null for a system-wide role.
   * The special value "*" refers to all buckets.
   */
  public Role(String roleName, String bucket) {
    this(roleName, bucket, null, null);
  }

  /**
   * Creates a new role. If all of the nullable parameters are null, a system-wide role is created.
   * Otherwise, the role applies to the specified bucket / scope / collection.
   *
   * @param roleName symbolic name of the role
   * @param bucket (nullable) The bucket this role applies to, or "*" if it applies to all buckets, or null if not applicable.
   * @param scope (nullable) The scope within the bucket, or null or "*" if the role applies to all scopes within the bucket.
   * @param collection (nullable) The collection within the scope, or null or "*" if the role applies to all collections within the scope.
   */
  @JsonCreator
  public Role(@JsonProperty("role") String roleName,
              @JsonProperty("bucket_name") String bucket,
              @JsonProperty("scope_name") String scope,
              @JsonProperty("collection_name") String collection) {
    this.name = requireNonNull(roleName);

    // Don't convert "*" to empty optional here, because we need to distinguish between
    // roles that apply to a bucket and roles that are system-wide.
    this.bucket = Optional.ofNullable(bucket);

    // Interpret either "*" or null to mean "all scopes" / "all collections".
    // Couchbase 7.0 and later will send an explicit wildcard "*".
    // Earlier servers won't send anything, so this will be null.
    this.scope = parseWildcardOptional(scope);
    this.collection = parseWildcardOptional(collection);

    if (this.scope.isPresent() && isNullOrWildcard(bucket)) {
      throw new IllegalArgumentException("When a scope is specified, the bucket cannot be null or wildcard");
    }
    if (this.collection.isPresent() && !this.scope.isPresent()) {
      throw new IllegalArgumentException("When a collection is specified, the scope cannot be null or wildcard.");
    }
  }

  private static boolean isNullOrWildcard(String s) {
    return s == null || "*".equals(s);
  }

  private static Optional<String> parseWildcardOptional(String s) {
    return isNullOrWildcard(s) ? Optional.empty() : Optional.of(s);
  }

  public String name() {
    return name;
  }

  public Optional<String> bucket() {
    return bucket;
  }

  public Optional<String> scope() {
    return scope;
  }

  public Optional<String> collection() {
    return collection;
  }

  @Override
  public String toString() {
    return format();
  }

  public String format() {
    return name + formatTarget();
  }

  private String formatTarget() {
    if (!bucket.isPresent()) {
      return "";
    }

    StringBuilder sb = new StringBuilder("[").append(bucket.get());
    scope.ifPresent(s -> sb.append(":").append(s));
    collection.ifPresent(s -> sb.append(":").append(s));
    return sb.append("]").toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Role role = (Role) o;
    return name.equals(role.name) &&
        bucket.equals(role.bucket) &&
        scope.equals(role.scope) &&
        collection.equals(role.collection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, bucket, scope, collection);
  }
}

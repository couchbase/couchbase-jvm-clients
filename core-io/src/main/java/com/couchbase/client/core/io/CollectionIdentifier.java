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

package com.couchbase.client.core.io;

import com.couchbase.client.core.annotation.Stability;
import reactor.util.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static java.util.Objects.requireNonNull;

/**
 * The {@link CollectionIdentifier} uniquely identifies the position of a collection.
 *
 * @since 2.0.0
 */
@Stability.Internal
public class CollectionIdentifier implements Serializable {

  public static final String DEFAULT_SCOPE = "_default";
  public static final String DEFAULT_COLLECTION = "_default";

  private final String bucket;
  private final boolean isDefault;
  private final @Nullable String scope;
  private final @Nullable String collection;

  public static CollectionIdentifier fromDefault(String bucket) {
    return new CollectionIdentifier(bucket, Optional.of(DEFAULT_SCOPE), Optional.of(DEFAULT_COLLECTION));
  }

  public CollectionIdentifier(String bucket, Optional<String> scope, Optional<String> collection) {
    requireNonNull(bucket);
    requireNonNull(scope);
    requireNonNull(collection);

    this.bucket = bucket;
    this.scope = scope.orElse(null);
    this.collection = collection.orElse(null);
    this.isDefault = Optional.of(DEFAULT_SCOPE).equals(scope) && Optional.of(DEFAULT_COLLECTION).equals(collection);
  }

  public String bucket() {
    return bucket;
  }

  public Optional<String> scope() {
    return Optional.ofNullable(scope);
  }

  public Optional<String> collection() {
    return Optional.ofNullable(collection);
  }

  public boolean isDefault() {
    return isDefault;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CollectionIdentifier that = (CollectionIdentifier) o;
    return Objects.equals(bucket, that.bucket) &&
      Objects.equals(scope, that.scope) &&
      Objects.equals(collection, that.collection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucket, scope, collection);
  }

  @Override
  public String toString() {
    return "CollectionIdentifier{" +
      "bucket='" + redactMeta(bucket) + '\'' +
      ", scope=" + redactMeta(scope) +
      ", collection=" + redactMeta(collection) +
      ", isDefault=" + redactMeta(isDefault) +
      '}';
  }

  public Map<String, Object> toMap() {
    Map<String, Object> exported = new HashMap<>();
    exported.put("bucket", redactMeta(bucket()));
    exported.put("scope", redactMeta(scope().orElse(CollectionIdentifier.DEFAULT_SCOPE)));
    exported.put("collection", redactMeta(collection().orElse(CollectionIdentifier.DEFAULT_COLLECTION)));
    return exported;
  }
}

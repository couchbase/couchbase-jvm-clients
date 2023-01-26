/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public final class CoreKeyspace {
  private final String bucket;
  private final String scope;
  private final String collection;

  private final boolean isDefaultCollection;
  private final CollectionIdentifier collectionIdentifier;

  public CoreKeyspace(String bucket, String scope, String collection) {
    this.bucket = requireNonNull(bucket);
    this.scope = requireNonNull(scope);
    this.collection = requireNonNull(collection);
    this.isDefaultCollection = scope.equals("_default") && collection.equals("_default");
    this.collectionIdentifier = new CollectionIdentifier(bucket, Optional.of(scope), Optional.of(collection));
  }

  public String bucket() {
    return bucket;
  }

  public String scope() {
    return scope;
  }

  public String collection() {
    return collection;
  }

  public boolean isDefaultCollection() {
    return isDefaultCollection;
  }

  public static CoreKeyspace from(CollectionIdentifier id) {
    return new CoreKeyspace(id.bucket(), id.scope().orElse("_default"), id.collection().orElse("_default"));
  }

  public CollectionIdentifier toCollectionIdentifier() {
    return collectionIdentifier;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CoreKeyspace keyspace = (CoreKeyspace) o;
    return bucket.equals(keyspace.bucket) && scope.equals(keyspace.scope) && collection.equals(keyspace.collection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucket, scope, collection);
  }

  @Override
  public String toString() {
    return bucket + ":" + scope + "." + collection;
  }
}

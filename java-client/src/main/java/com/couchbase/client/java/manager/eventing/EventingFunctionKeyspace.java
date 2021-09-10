/*
 * Copyright 2021 Couchbase, Inc.
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
package com.couchbase.client.java.manager.eventing;

import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.Collection;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * A keyspace represents a triple of bucket, scope and collection.
 */
public class EventingFunctionKeyspace {

  private final String bucket;
  private final String scope;
  private final String collection;

  private EventingFunctionKeyspace(final String bucket, final String scope, final String collection) {
    this.bucket = notNull(bucket, "bucket");
    this.scope = scope == null || scope.isEmpty() ? CollectionIdentifier.DEFAULT_SCOPE : scope;
    this.collection = collection == null || collection.isEmpty() ? CollectionIdentifier.DEFAULT_COLLECTION : collection;
  }

  public static EventingFunctionKeyspace create(final String bucket) {
    return new EventingFunctionKeyspace(bucket, null, null);
  }

  public static EventingFunctionKeyspace create(final String bucket, final String collection) {
    return new EventingFunctionKeyspace(bucket, null, collection);
  }

  public static EventingFunctionKeyspace create(final String bucket, final String scope, final String collection) {
    return new EventingFunctionKeyspace(bucket, scope, collection);
  }

  public static EventingFunctionKeyspace create(Collection collection) {
    return new EventingFunctionKeyspace(collection.bucketName(), collection.scopeName(), collection.name());
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

  @Override
  public String toString() {
    return "EventingFunctionKeyspace{" +
      "bucket='" + bucket + '\'' +
      ", scope='" + scope + '\'' +
      ", collection='" + collection + '\'' +
      '}';
  }
}

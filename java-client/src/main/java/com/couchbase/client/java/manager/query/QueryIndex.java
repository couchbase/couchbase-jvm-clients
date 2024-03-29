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

package com.couchbase.client.java.manager.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.manager.CoreQueryIndex;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.util.Objects;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbObjects.defaultIfNull;
import static com.couchbase.client.core.util.CbStrings.emptyToNull;
import static java.util.Objects.requireNonNull;

/**
 * Contains the properties of a Query Index.
 */
public class QueryIndex {
  private final CoreQueryIndex internal;

  @Stability.Internal
  QueryIndex(final CoreQueryIndex internal) {
    this.internal = requireNonNull(internal);
  }

  /**
   * True if this index is a primary index.
   *
   * @return true if this index is a primary index.
   */
  public boolean primary() {
    return internal.primary();
  }

  /**
   * Returns the name of this index.
   *
   * @return the name of the index.
   */
  public String name() {
    return internal.name();
  }

  /**
   * Returns the index type (most likely "gsi").
   *
   * @return the type of the index.
   */
  public String type() {
    return internal.type();
  }

  /**
   * Returns the state in which the index is in (i.e. "online").
   *
   * @return the state of the index.
   */
  public String state() {
    return internal.state();
  }

  /**
   * Returns the keyspace of this index.
   * <p>
   * If the index is at the bucket-level, this will return the bucket name. If the index is at the collection-level,
   * the keyspace is the name of the collection.
   *
   * @return the keyspace of this index.
   */
  public String keyspace() {
    return internal.keyspace();
  }

  /**
   * Returns the namespace of this index.
   * <p>
   * The namespace should not be confused with the keyspace - the namespace usually is "default".
   *
   * @return the namespace of this index.
   */
  public String namespace() {
    return internal.namespace();
  }

  /**
   * Returns an {@link JsonArray array} of Strings that represent the index key(s).
   * <p>
   * The array is empty in the case of a PRIMARY INDEX.
   * <p>
   * Note that the query service can present the key in a slightly different manner from when you declared the index:
   * for instance, it will show the indexed fields in an escaped format (surrounded by backticks).
   *
   * @return an array of Strings that represent the index key(s), or an empty array in the case of a PRIMARY index.
   */
  public JsonArray indexKey() {
    return JsonArray.fromJson(internal.indexKey().toString());
  }

  /**
   * Returns the {@link String} representation of the index's condition (the WHERE clause of the index), or an empty
   * Optional if no condition was set.
   * <p>
   * Note that the query service can present the condition in a slightly different manner from when you declared the index:
   * for instance it will wrap expressions with parentheses and show the fields in an escaped format (surrounded by
   * backticks).
   *
   * @return the condition/WHERE clause of the index or empty string if none.
   */
  public Optional<String> condition() {
    return internal.condition();
  }

  /**
   * If present, returns the configured partition for the index.
   *
   * @return the partition if set, empty if none.
   */
  public Optional<String> partition() {
    return internal.partition();
  }

  /**
   * If present, returns the name of the scope this index is stored in.
   *
   * @return the name of the scope, if present.
   */
  public Optional<String> scopeName() {
    return internal.scopeName();
  }

  /**
   * If present, returns the name of the bucket this index is stored in.
   *
   * @return the name of the bucket, if present.
   */
  public String bucketName() {
    return internal.bucketName();
  }

  /**
   * If present, returns the name of the collection this index is stored in.
   *
   * @return the name of the collection, if present.
   */
  public Optional<String> collectionName() {
    return internal.collectionName();
  }

  /**
   * Returns the JSON as it arrived from the server.
   *
   * @return the raw JSON representation of the index information, as returned by the query service.
   */
  public JsonObject raw() {
    return JsonObject.fromJson(internal.raw().toString());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryIndex that = (QueryIndex) o;
    return raw().equals(that.raw());
  }

  @Override
  public int hashCode() {
    return Objects.hash(raw());
  }

  @Override
  public String toString() {
    return redactMeta(raw()).toString();
  }
}

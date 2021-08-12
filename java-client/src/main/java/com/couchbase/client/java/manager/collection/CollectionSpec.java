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

package com.couchbase.client.java.manager.collection;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;

import java.time.Duration;
import java.util.Objects;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * The {@link CollectionSpec} describes properties of a collection that can be managed.
 */
public class CollectionSpec {

  /**
   * The name of the collection.
   */
  private final String name;

  /**
   * The name of the parent scope.
   */
  private final String scopeName;

  /**
   * The maximum expiration time configured on the collection as a whole.
   */
  private final Duration maxExpiry;

  /**
   * Creates a new {@link CollectionSpec}.
   *
   * @param name the name of the collection.
   * @param scopeName the name of the parent scope.
   * @param maxExpiry the maximum expiry, or {@link Duration#ZERO} if none.
   */
  private CollectionSpec(String name, String scopeName, Duration maxExpiry) {
    this.name = notNullOrEmpty(name, "Name");
    this.scopeName = notNullOrEmpty(scopeName, "Scope Name");
    this.maxExpiry = notNull(maxExpiry, "Max Expiry");
  }

  /**
   * Creates a new {@link CollectionSpec} using the default scope.
   *
   * @param name the name of the collection.
   * @return the created {@link CollectionSpec}.
   */
  public static CollectionSpec create(String name) {
    return create(name, CollectionIdentifier.DEFAULT_SCOPE);
  }

  /**
   * Creates a new {@link CollectionSpec} with default properties.
   *
   * @param name the name of the collection.
   * @param scopeName the name of the parent scope.
   * @return the created {@link CollectionSpec}.
   */
  public static CollectionSpec create(final String name, final String scopeName) {
    return new CollectionSpec(name, scopeName, Duration.ZERO);
  }

  /**
   * Creates a new {@link CollectionSpec} with a custom max expiry on the default scope.
   *
   * @param name the name of the collection.
   * @param maxExpiry the maximum expiry (ttl) to use for this collection.
   * @return the created {@link CollectionSpec}.
   */
  @Stability.Volatile
  public static CollectionSpec create(final String name, final Duration maxExpiry) {
    return create(name, CollectionIdentifier.DEFAULT_SCOPE, maxExpiry);
  }

  /**
   * Creates a new {@link CollectionSpec} with a custom max expiry.
   *
   * @param name the name of the collection.
   * @param scopeName the name of the parent scope.
   * @param maxExpiry the maximum expiry (ttl) to use for this collection.
   * @return the created {@link CollectionSpec}.
   */
  @Stability.Volatile
  public static CollectionSpec create(final String name, final String scopeName, final Duration maxExpiry) {
    return new CollectionSpec(name, scopeName, maxExpiry);
  }

  /**
   * The name of the collection.
   */
  public String name() {
    return name;
  }

  /**
   * The name of the parent scope.
   */
  public String scopeName() {
    return scopeName;
  }

  /**
   * The max expiry for this collection, {@link Duration#ZERO} otherwise.
   */
  @Stability.Volatile
  public Duration maxExpiry() {
    return maxExpiry;
  }

  @Override
  public String toString() {
    return "CollectionSpec{" +
      "name='" + redactMeta(name) + '\'' +
      ", scopeName='" + redactMeta(scopeName) + '\'' +
      ", maxExpiry=" + redactMeta(maxExpiry.getSeconds()) +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CollectionSpec that = (CollectionSpec) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(scopeName, that.scopeName) &&
      Objects.equals(maxExpiry, that.maxExpiry);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, scopeName, maxExpiry);
  }

}

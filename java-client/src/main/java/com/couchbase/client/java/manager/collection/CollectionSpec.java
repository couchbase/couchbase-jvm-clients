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

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import reactor.util.annotation.Nullable;

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
   * A special collection "max expiry" value that means the collection's
   * max expiry is always the same as the bucket's max expiry.
   * <p>
   * Use the bucket management API to discover the actual expiry value.
   */
  @Stability.Volatile
  public static final Duration SAME_EXPIRY_AS_BUCKET = Duration.ZERO;

  /**
   * A special collection "max expiry" value that means documents in the collection
   * never expire, regardless of the bucket's max expiry setting.
   * <p>
   * Requires Couchbase Server 7.6 or later.
   */
  @SinceCouchbase("7.6")
  @Stability.Volatile
  public static final Duration NEVER_EXPIRE = Duration.ofSeconds(-1);

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
   * Whether history retention is enabled on this collection.  Older server versions will not have this setting, in which case it will be null.
   */
  private final Boolean history;

  /**
   * Creates a new {@link CollectionSpec}.
   *
   * @param name the name of the collection.
   * @param scopeName the name of the parent scope.
   * @param maxExpiry the maximum expiry, or {@link #NEVER_EXPIRE}, or {@link #SAME_EXPIRY_AS_BUCKET}.
   * @param history whether history retention is enabled on this collection.
   */
  private CollectionSpec(String name, String scopeName, Duration maxExpiry, @Nullable Boolean history) {
    this.name = notNullOrEmpty(name, "Name");
    this.scopeName = notNullOrEmpty(scopeName, "Scope Name");
    this.maxExpiry = internExpiry(notNull(maxExpiry, "Max Expiry"));
    this.history = history;
  }

  /**
   * Allows using reference equality (==) to compare {@link #maxExpiry()}
   * against the special expiry values.
   */
  private static Duration internExpiry(Duration d) {
    if (d.equals(SAME_EXPIRY_AS_BUCKET)) {
      return SAME_EXPIRY_AS_BUCKET;
    }
    if (d.equals(NEVER_EXPIRE)) {
      return NEVER_EXPIRE;
    }
    return d;
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
    return new CollectionSpec(name, scopeName, SAME_EXPIRY_AS_BUCKET, null);
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
    return new CollectionSpec(name, scopeName, maxExpiry, null);
  }

  /**
   * Creates a new {@link CollectionSpec} with a custom max expiry.
   *
   * @param name the name of the collection.
   * @param scopeName the name of the parent scope.
   * @param maxExpiry the maximum expiry (ttl) to use for this collection.
   * @param history whether history retention is enabled on this collection.
   * @return the created {@link CollectionSpec}.
   */
  @Stability.Volatile
  @Stability.Internal
  protected static CollectionSpec internalCreate(final String name, final String scopeName, final Duration maxExpiry, final Boolean history) {
    return new CollectionSpec(name, scopeName, maxExpiry, history);
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
   * The max expiry for documents in this collection,
   * or a non-positive duration indicating special expiry behavior.
   *
   * @see #SAME_EXPIRY_AS_BUCKET
   * @see #NEVER_EXPIRE
   */
  @Stability.Volatile
  public Duration maxExpiry() {
    return maxExpiry;
  }

  /**
   * whether history retention is enabled on this collection.
   */
  @Stability.Volatile
  public Boolean history() {
    return history;
  }

  @Override
  public String toString() {
    return "CollectionSpec{" +
      "name='" + redactMeta(name) + '\'' +
      ", scopeName='" + redactMeta(scopeName) + '\'' +
      ", maxExpiry=" + redactMeta(maxExpiry.getSeconds()) +
      ", history=" + redactMeta(history) +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CollectionSpec that = (CollectionSpec) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(scopeName, that.scopeName) &&
      Objects.equals(maxExpiry, that.maxExpiry) &&
      Objects.equals(history, that.history);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, scopeName, maxExpiry, history);
  }

}

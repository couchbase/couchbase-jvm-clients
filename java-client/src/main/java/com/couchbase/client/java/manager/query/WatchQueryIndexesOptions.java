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
import com.couchbase.client.core.api.manager.CoreScopeAndCollection;
import com.couchbase.client.core.api.manager.CoreWatchQueryIndexesOptions;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.java.CommonOptions;

import java.util.Optional;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Allows customizing how the query indexes are watched.
 */
public class WatchQueryIndexesOptions extends CommonOptions<WatchQueryIndexesOptions> {

  private boolean watchPrimary;
  private String scopeName;
  private String collectionName;

  private WatchQueryIndexesOptions() {
  }

  /**
   * Creates a new instance with default values.
   *
   * @return the instantiated default options.
   */
  public static WatchQueryIndexesOptions watchQueryIndexesOptions() {
    return new WatchQueryIndexesOptions();
  }

  /**
   * Set to true if the primary index should be included in the watch result.
   * <p>
   * Note that if the bucket has no primary index, the watch operation will fail with a {@link IndexNotFoundException}.
   *
   * @deprecated `collection.queryIndexes()` should now be used for collection-related query index operations.
   * @param watchPrimary if the primary index should be included in the watch.
   * @return this options class for chaining purposes.
   */
  public WatchQueryIndexesOptions watchPrimary(final boolean watchPrimary) {
    this.watchPrimary = watchPrimary;
    return this;
  }

  /**
   * Sets the scope name for this query management operation.
   * <p>
   * Please note that if the scope name is set, the {@link #collectionName(String)} (String)} must also be set.
   *
   * @deprecated `collection.queryIndexes()` should now be used for collection-related query index operations.
   * @param scopeName the name of the scope.
   * @return this options class for chaining purposes.
   */
  @Deprecated
  public WatchQueryIndexesOptions scopeName(final String scopeName) {
    this.scopeName = notNullOrEmpty(scopeName, "ScopeName");
    return this;
  }

  /**
   * Sets the collection name for this query management operation.
   * <p>
   * Please note that if the collection name is set, the {@link #scopeName(String)} must also be set.
   *
   * @param collectionName the name of the collection.
   * @return this options class for chaining purposes.
   */
  @Deprecated
  public WatchQueryIndexesOptions collectionName(final String collectionName) {
    this.collectionName = notNullOrEmpty(collectionName, "CollectionName");
    return this;
  }

  @Stability.Internal
  public Built build() {
    if (collectionName != null && scopeName == null) {
      throw InvalidArgumentException.fromMessage("If a collectionName is provided, a scopeName must also be provided");
    }
    if (scopeName != null && collectionName == null) {
      throw InvalidArgumentException.fromMessage("If a scopeName is provided, a collectionName must also be provided");
    }
    return new Built();
  }

  public class Built extends BuiltCommonOptions implements CoreWatchQueryIndexesOptions {

    Built() {
    }

    @Override
    public boolean watchPrimary() {
      return watchPrimary;
    }

    @Override
    public CoreScopeAndCollection scopeAndCollection() {
      if (scopeName != null && collectionName != null) {
        return new CoreScopeAndCollection(scopeName, collectionName);
      }
      return null;
    }

    @Override
    public CoreCommonOptions commonOptions() {
      return this;
    }
  }
}

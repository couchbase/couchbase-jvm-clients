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
import com.couchbase.client.core.api.manager.CoreDropPrimaryQueryIndexOptions;
import com.couchbase.client.core.api.manager.CoreScopeAndCollection;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.java.CommonOptions;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Allows customizing how primary indexes are dropped.
 */
public class DropPrimaryQueryIndexOptions extends CommonOptions<DropPrimaryQueryIndexOptions> {

  private boolean ignoreIfNotExists;
  private String scopeName;
  private String collectionName;

  private DropPrimaryQueryIndexOptions() {
  }

  /**
   * Creates a new instance with default values.
   *
   * @return the instantiated default options.
   */
  public static DropPrimaryQueryIndexOptions dropPrimaryQueryIndexOptions() {
    return new DropPrimaryQueryIndexOptions();
  }

  /**
   * If the index does not exist, an exception will be thrown unless this is set to true.
   */
  public DropPrimaryQueryIndexOptions ignoreIfNotExists(boolean ignore) {
    this.ignoreIfNotExists = ignore;
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
  public DropPrimaryQueryIndexOptions scopeName(final String scopeName) {
    this.scopeName = notNullOrEmpty(scopeName, "ScopeName");
    return this;
  }

  /**
   * Sets the collection name for this query management operation.
   * <p>
   * Please note that if the collection name is set, the {@link #scopeName(String)} must also be set.
   *
   * @deprecated `collection.queryIndexes()` should now be used for collection-related query index operations.
   * @param collectionName the name of the collection.
   * @return this options class for chaining purposes.
   */
  @Deprecated
  public DropPrimaryQueryIndexOptions collectionName(final String collectionName) {
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

  public class Built extends BuiltCommonOptions implements CoreDropPrimaryQueryIndexOptions {

    Built() {
    }

    @Override
    public boolean ignoreIfNotExists() {
      return ignoreIfNotExists;
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

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
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.java.CommonOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

public class CreateQueryIndexOptions extends CommonOptions<CreateQueryIndexOptions> {

  private final Map<String, Object> with = new HashMap<>();

  private Optional<String> indexName = Optional.empty();
  private boolean ignoreIfExists;
  private String scopeName;
  private String collectionName;

  private CreateQueryIndexOptions() {
  }

  public static CreateQueryIndexOptions createQueryIndexOptions() {
    return new CreateQueryIndexOptions();
  }

  /**
   * If an index with the same name already exists, an exception will be thrown unless this is set to true.
   */
  public CreateQueryIndexOptions ignoreIfExists(boolean ignore) {
    this.ignoreIfExists = ignore;
    return this;
  }

  /**
   * Specifies the number of replicas of the index to create.
   */
  public CreateQueryIndexOptions numReplicas(int numReplicas) {
    // yeah, there's no "s" in the option name
    return with("num_replica", numReplicas);
  }

  /**
   * Set to {@code true} to defer building of the index until
   * {@link QueryIndexManager#buildDeferredIndexes} is called.
   * <p>
   * If you are creating multiple indexes on the same bucket,
   * you may see improved performance by creating them in deferred
   * mode and then building them all at once.
   */
  public CreateQueryIndexOptions deferred(boolean deferred) {
    return with("defer_build", deferred);
  }

  /**
   * Escape hatch for specifying extra options in the {@code WITH} clause.
   * Intended for options that are supported by Couchbase Server but not by this
   * version of the SDK.
   */
  public CreateQueryIndexOptions with(String optionName, Object optionValue) {
    this.with.put(optionName, optionValue);
    return this;
  }

  /**
   * Sets the scope name for this query management operation.
   * <p>
   * Please note that if the scope name is set, the {@link #collectionName(String)} (String)} must also be set.
   *
   * @param scopeName the name of the scope.
   * @return this options class for chaining purposes.
   */
  @Stability.Uncommitted
  public CreateQueryIndexOptions scopeName(final String scopeName) {
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
  @Stability.Uncommitted
  public CreateQueryIndexOptions collectionName(final String collectionName) {
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

  public class Built extends BuiltCommonOptions {
    Built() { }
    public boolean ignoreIfExists() {
      return ignoreIfExists;
    }

    public Map<String, Object> with() {
      return with;
    }

    public Optional<String> scopeName() {
      return Optional.ofNullable(scopeName);
    }

    public Optional<String> collectionName() {
      return Optional.ofNullable(collectionName);
    }
  }
}

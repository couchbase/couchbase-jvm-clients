/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.java;

import static com.couchbase.client.java.AsyncBucket.DEFAULT_COLLECTION;
import static com.couchbase.client.java.AsyncUtils.block;

/**
 * The scope identifies a group of collections and allows high application
 * density as a result.
 *
 * <p>If no scope is explicitly provided, the default scope is used.</p>
 *
 * @since 3.0.0
 */
public class Scope {

  /**
   * The underlying async scope which actually performs the actions.
   */
  private final AsyncScope asyncScope;

  /**
   * Creates a new {@link Scope}.
   *
   * @param asyncScope the underlying async scope.
   */
  Scope(final AsyncScope asyncScope) {
    this.asyncScope = asyncScope;
  }

  /**
   * The name of the scope.
   *
   * @return the name of the scope.
   */
  public String name() {
    return asyncScope.name();
  }

  /**
   * Opens the default collection for this scope.
   *
   * @return the default collection once opened.
   */
  public Collection defaultCollection() {
    return collection(DEFAULT_COLLECTION);
  }

  /**
   * Opens a collection for this scope with an explicit name.
   *
   * @param name the collection name.
   * @return the requested collection if successful.
   */
  public Collection collection(final String name) {
    return new Collection(block(asyncScope.collection(name)));
  }

}

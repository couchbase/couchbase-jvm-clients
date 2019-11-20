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

import java.util.List;

import static com.couchbase.client.java.AsyncUtils.block;

/**
 * The {@link CollectionManager} provides APIs to manage bucket collections and scopes.
 */
@Stability.Volatile
public class CollectionManager {

  private final AsyncCollectionManager asyncCollectionManager;

  public CollectionManager(AsyncCollectionManager asyncCollectionManager) {
    this.asyncCollectionManager = asyncCollectionManager;
  }

  /**
   * Creates a collection if it does not already exist.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @throws CollectionAlreadyExistsException if the collection already exists
   * @throws ScopeNotFoundException if the specified scope does not exist.
   */
  public void createCollection(final CollectionSpec collectionSpec) {
    block(asyncCollectionManager.createCollection(collectionSpec));
  }

  /**
   * Drops a collection if it exists.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @throws CollectionNotFoundException if the collection did not exist.
   * @throws ScopeNotFoundException if the specified scope does not exist.
   */
  public void dropCollection(final CollectionSpec collectionSpec) {
    block(asyncCollectionManager.dropCollection(collectionSpec));
  }

  /**
   * Creates a scope if it does not already exist.
   *
   * @param scopeName the name of the scope to create.
   * @throws ScopeAlreadyExistsException if the scope already exists.
   */
  public void createScope(final String scopeName) {
    block(asyncCollectionManager.createScope(scopeName));
  }

  /**
   * Drops a scope if it exists.
   *
   * @param scopeName the name of the scope to drop.
   * @throws ScopeNotFoundException if the scope did not exist.
   */
  public void dropScope(final String scopeName) {
    block(asyncCollectionManager.dropScope(scopeName));
  }

  /**
   * Returns the scope if it exists.
   *
   * @param scopeName the name of the scope.
   * @return information about the requested scope.
   * @throws ScopeNotFoundException if scope does not exist
   */
  public ScopeSpec getScope(final String scopeName) {
    return block(asyncCollectionManager.getScope(scopeName));
  }

  /**
   * Returns all scopes in this bucket.
   *
   * @return a list of all scopes in this bucket.
   */
  public List<ScopeSpec> getAllScopes() {
    return block(asyncCollectionManager.getAllScopes());
  }

}

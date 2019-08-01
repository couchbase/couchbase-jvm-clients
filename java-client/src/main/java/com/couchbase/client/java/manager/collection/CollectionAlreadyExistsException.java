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
import com.couchbase.client.core.error.CouchbaseException;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static java.util.Objects.requireNonNull;

@Stability.Volatile
public class CollectionAlreadyExistsException extends CouchbaseException {
  private final String collectionName;

  public CollectionAlreadyExistsException(String collectionName) {
    super("Collection [" + redactSystem(collectionName) + "] already exists.");
    this.collectionName = requireNonNull(collectionName);
  }

  public static CollectionAlreadyExistsException forCollection(String collectionName) {
    return new CollectionAlreadyExistsException(collectionName);
  }

  public String collectionName() {
    return collectionName;
  }
}

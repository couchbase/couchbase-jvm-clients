/*
 * Copyright (c) 2016 - 2019 Couchbase, Inc.
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

package com.couchbase.client.core.error;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

/**
 * Indicates an optimistic locking failure.
 * <p>
 * The operation failed because the specified compare and swap (CAS) value
 * differs from the document's actual CAS value. This means the document
 * was modified since the original CAS value was acquired.
 * <p>
 * The application should usually respond by fetching a fresh version
 * of the document and repeating the failed operation.
 *
 * @since 2.0
 */
public class CASMismatchException extends CouchbaseException {
  private final String key;

  private CASMismatchException(String key) {
    super("Document with key [" + redactUser(key) + "] has been concurrently modified");
    this.key = key;
  }

  public static CASMismatchException forKey(String key) {
    return new CASMismatchException(key);
  }

  public String key() {
    return key;
  }
}

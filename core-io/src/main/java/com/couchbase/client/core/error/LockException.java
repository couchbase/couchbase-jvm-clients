/*
 * Copyright (c) 2019 Couchbase, Inc.
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
 * Thrown when the server reports a temporary failure that
 * is very likely to be lock-related (like an already locked
 * key or a bad cas used for unlock).
 *
 * <p>See <a href="https://issues.couchbase.com/browse/MB-13087">this issue</a>
 * for an explanation of why this is only <i>likely</i> to be lock-related.</p>
 *
 * @since 3.0
 */
public class LockException extends CouchbaseException implements RetryableOperationException {
  private final String key;

  private LockException(String key) {
    super("Failed to acquire or release the lock on key [" + redactUser(key) + "]");
    this.key = key;
  }

  public static LockException forKey(String key) {
    return new LockException(key);
  }

  public String key() {
    return key;
  }
}

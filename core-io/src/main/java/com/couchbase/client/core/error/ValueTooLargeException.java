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
 * Thrown when the request is too big for some reason.
 *
 * @since 3.0
 */
public class ValueTooLargeException extends CouchbaseException {
  private final String key;

  private ValueTooLargeException(String key) {
    super("The value for key [" + redactUser(key) + "] is too large.");
    this.key = key;
  }

  public static ValueTooLargeException forKey(String key) {
    return new ValueTooLargeException(key);
  }

  public String key() {
    return key;
  }
}

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

public class DocumentMutationLostException extends CouchbaseException {
  private final String key;

  private DocumentMutationLostException(String key) {
    super("Mutation for key [" + redactUser(key) + "] was lost (possibly due to failover)");
    this.key = key;
  }

  public static DocumentMutationLostException forKey(String key) {
    return new DocumentMutationLostException(key);
  }

  public String key() {
    return key;
  }
}

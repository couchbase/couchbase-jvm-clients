/*
 * Copyright (c) 2023 Couchbase, Inc.
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

import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.context.KeyValueErrorContext;

/**
 * Thrown when the server reports the document is already locked - generally raised when an unlocking operation is
 * being performed.
 *
 * @since 3.5.1
 */
public class DocumentNotLockedException extends CouchbaseException {

  public DocumentNotLockedException(KeyValueErrorContext ctx) {
    this((ErrorContext) ctx);
  }

  public DocumentNotLockedException(ErrorContext ctx) {
    super("Server indicates the document is not locked", ctx);
  }
}

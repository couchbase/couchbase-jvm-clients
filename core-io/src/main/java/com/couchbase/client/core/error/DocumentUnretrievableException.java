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

import com.couchbase.client.core.error.context.ErrorContext;

/**
 * Indicates an operation completed but no successful document was retrievable.
 *
 * @since 2.0
 */
public class DocumentUnretrievableException extends CouchbaseException {

  public DocumentUnretrievableException(final ErrorContext ctx) {
    super("No document retrievable with a successful status", ctx);
  }

  public DocumentUnretrievableException(final String message, final ErrorContext ctx) {
    super(message, ctx);
  }

  public static DocumentUnretrievableException noReplicasSuitable() {
    return new DocumentUnretrievableException("No suitable replicas were available.  Note that it is advised to always have a try-catch fallback to e.g. a regular get, when using replica gets", null);
  }
}

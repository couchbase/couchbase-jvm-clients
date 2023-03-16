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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.context.ErrorContext;

/**
 * Indicates an operation failed because there has been an internal error in the server.
 *
 * @since 3.0
 */
public class InternalServerFailureException extends CouchbaseException {

  public InternalServerFailureException(final ErrorContext ctx) {
    super("Internal Couchbase Server error", ctx);
  }

  @Stability.Internal
  public InternalServerFailureException(final Throwable cause, final ErrorContext ctx) {
    super("Internal Couchbase Server error", cause, ctx);
  }

}

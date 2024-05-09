/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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

import static java.util.Objects.requireNonNull;

/**
 * This is an internal exception the user should never see;
 * The SDK should translate it to some other exception before
 * presenting it to the user.
 */
@Stability.Internal
public class CoreErrorCodeAndMessageException extends CouchbaseException {
  private final ErrorCodeAndMessage errorCodeAndMessage;

  public CoreErrorCodeAndMessageException(
    ErrorCodeAndMessage errorCodeAndMessage,
    ErrorContext errorContext
  ) {
    super(errorCodeAndMessage.toString(), errorContext);
    this.errorCodeAndMessage = requireNonNull(errorCodeAndMessage);
  }

  public ErrorCodeAndMessage errorCodeAndMessage() {
    return errorCodeAndMessage;
  }
}

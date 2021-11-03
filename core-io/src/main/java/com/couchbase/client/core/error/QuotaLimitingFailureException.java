/*
 * Copyright (c) 2021 Couchbase, Inc.
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
 * This error is raised if the operation failed due to hitting a quota-limit on the server side.
 * <p>
 * Note that this quota-limit might be implicitly configured if you are using Couchbase Capella (for example when
 * using the free tier). See the error context with the exception for further information on the exact cause and
 * check the documentation for potential remedy.
 */
@Stability.Uncommitted
public class QuotaLimitingFailureException extends CouchbaseException {

  public QuotaLimitingFailureException(final ErrorContext errorContext) {
    super("Operation failed due to reaching a quota limit. See the error context for further details.", errorContext);
  }

}

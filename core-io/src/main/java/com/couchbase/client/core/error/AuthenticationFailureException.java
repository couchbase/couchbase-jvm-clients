/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import reactor.util.annotation.Nullable;

/**
 * Every exception that has to do with authentication problems should either
 * instantiate or subclass from this type.
 * <p>
 * Note that some, for intentional security reasons, it is not always
 * possible to disambiguate some failure conditions.  So this exception
 * can have multiple causes:
 * <p>
 * - Incorrect credentials have been supplied.
 * - The bucket being accessed does not exist.
 * - The bucket being accessed has been hibernated.
 *
 * @since 2.0.0
 */
public class AuthenticationFailureException extends CouchbaseException {

  public AuthenticationFailureException(String message, ErrorContext ctx, Throwable cause) {
    super(message, cause, ctx);
  }


  /**
   * Must only be called on an AUTHENTICATION_ERROR RetryReason.
   */
  public static AuthenticationFailureException onAuthError(Request<? extends Response> request, Throwable cause) {
    return onAuthError(new CancellationErrorContext(request.context()), cause);
  }

  public static AuthenticationFailureException onAuthError(CancellationErrorContext errorContext, @Nullable Throwable cause) {
    // Usually the reasons would include bucket not existing or being hibernated.  But this code path is only followed on
    // AUTHENTICATION_ERROR, which can only be raised if the GCCCP connection is failing to authenticate.  So, it's unrelated to
    // any bucket.
    return new AuthenticationFailureException("The user does not have the right credentials to access the cluster",
      new CancellationErrorContext(errorContext),
      cause);
  }
}

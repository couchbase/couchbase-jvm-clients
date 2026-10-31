/*
 * Copyright (c) 2026 Couchbase, Inc.
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

package com.couchbase.client.core.classic.query.conversational;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.ChatCurrentlyUnavailableException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.RateLimitedException;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.context.QueryErrorContext;
import reactor.util.annotation.Nullable;

@Stability.Internal
public class ConversationalQueryErrors {
  // Query error codes that SDK-RFC 0086 gives specific handling.
  public static final int NATURAL_LANGUAGE_QUEUE_FULL = 19220;
  public static final int NATURAL_LANGUAGE_ADMISSION_TIMEOUT = 19221;
  public static final int NATURAL_LANGUAGE_DISABLED = 19222;
  public static final int CHAT_NOT_LIVE = 19236;
  public static final int CHAT_NO_LIVE_SLOTS = 19240;
  public static final int CHAT_OWNERSHIP_CHECK_FAILED = 19241;
  public static final int CHAT_RESUME_FAILED = 19243;
  public static final int CHAT_ALREADY_LIVE = 19255;

  private ConversationalQueryErrors() {
  }

  /**
   * Remaps errors returned by the query service, per the SDK-RFC.  Any other error is returned unchanged.
   */
  static Throwable remap(Throwable err) {
    if (err instanceof CouchbaseException) {
      ErrorContext ctx = ((CouchbaseException) err).context();
      // This is the least-ugly way to perform the special case error handling needed by the RFC.
      if (ctx instanceof QueryErrorContext && !((QueryErrorContext) ctx).errors().isEmpty()) {
        QueryErrorContext errorContext = (QueryErrorContext) ctx;
        // The first top-level error determines the handling.
        CouchbaseException mapped = toThrowable(errorContext.errors().get(0), errorContext);
        if (mapped != null) {
          return mapped;
        }
      }
    }
    return err;
  }

  private static @Nullable CouchbaseException toThrowable(ErrorCodeAndMessage error, QueryErrorContext errorContext) {
    int code = error.code();

    // We need the reason code for this potential error:
    // {"code":19243,"msg":"Resume failed: %s","reason":{"_level":"exception","caller":"nlquery:527","code":19240,"key":"natural.service_natural_request_chat","message":"The cache for active chat sessions is full, cannot start a new chat session at the moment"}}
    int reasonCode = error.reason() != null && error.reason().get("code") instanceof Number
      ? ((Number) error.reason().get("code")).intValue()
      : 0;

    if (code == NATURAL_LANGUAGE_QUEUE_FULL
        || code == NATURAL_LANGUAGE_ADMISSION_TIMEOUT
        || code == CHAT_NO_LIVE_SLOTS
        || (code == CHAT_RESUME_FAILED && reasonCode == CHAT_NO_LIVE_SLOTS)) {
      return new RateLimitedException(errorContext);
    } else if (code == NATURAL_LANGUAGE_DISABLED) {
      return new FeatureNotAvailableException("Natural-language processing is disabled on the query node", errorContext);
    } else if (code == CHAT_NOT_LIVE) {
      return new ChatCurrentlyUnavailableException("The query node has no live chat with this id", errorContext);
    } else if (code == CHAT_OWNERSHIP_CHECK_FAILED) {
      return new AuthenticationFailureException("Failed the chat ownership check (this can indicate that the chat was created by one Couchbase user, and another user is trying to interact with it)", errorContext, null);
    }
    // Per RFC, CHAT_RESUME_FAILED and CHAT_ALREADY_LIVE are explicitly not handled specially here.
    return null;
  }
}

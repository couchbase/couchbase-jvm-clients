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

package com.couchbase.client.core.error;

import com.couchbase.client.core.error.context.ErrorContext;
import reactor.util.annotation.Nullable;

/**
 * Raised when a conversational query chat is no longer live on its query node.
 * <p>
 * This happens if the query service reports that it has no live chat with that id, or the node is no longer in the
 * cluster configuration.
 */
public class ChatCurrentlyUnavailableException extends CouchbaseException {

  public ChatCurrentlyUnavailableException(final String message, @Nullable final ErrorContext ctx) {
    super(message, ctx);
  }
}

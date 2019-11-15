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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.DurableWriteReCommitInProgressException;
import com.couchbase.client.core.error.KeyValueErrorContext;
import com.couchbase.client.core.error.ServerOutOfMemoryException;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.java.codec.JsonSerializer;

import java.util.concurrent.CompletableFuture;

public class LookupInAccessor {

  public static CompletableFuture<LookupInResult> lookupInAccessor(final String id,
                                                                   final Core core,
                                                                   final SubdocGetRequest request,
                                                                   final JsonSerializer serializer) {
    core.send(request);
    return request
      .response()
      .thenApply(response -> {
        if (response.status().success()) {
          return new LookupInResult(response.values(), response.cas(), serializer, null);
        }
        final KeyValueErrorContext ctx = KeyValueErrorContext.completedRequest(request, response.status());
        switch (response.status()) {
          case SUBDOC_FAILURE: return new LookupInResult(response.values(), response.cas(), serializer, ctx);
          case NOT_FOUND: throw new DocumentNotFoundException(ctx);
          case OUT_OF_MEMORY: throw new ServerOutOfMemoryException(ctx);
          case SYNC_WRITE_RE_COMMIT_IN_PROGRESS: throw new DurableWriteReCommitInProgressException(ctx);
          case TEMPORARY_FAILURE: // intended fallthrough to the case below
          case SERVER_BUSY: throw new TemporaryFailureException(ctx);
          default: throw new CouchbaseException("LookupIn operation failed", ctx);
        }
      });
  }
}

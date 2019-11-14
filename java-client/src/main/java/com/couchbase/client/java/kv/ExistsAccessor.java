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
import com.couchbase.client.core.error.DefaultErrorUtil;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.DurableWriteReCommitInProgressException;
import com.couchbase.client.core.error.KeyValueErrorContext;
import com.couchbase.client.core.error.ServerOutOfMemoryException;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.core.msg.kv.ObserveViaCasRequest;
import com.couchbase.client.core.msg.kv.ObserveViaCasResponse;

import java.util.concurrent.CompletableFuture;

public class ExistsAccessor {

  public static CompletableFuture<ExistsResult> exists(final String key, final Core core,
                                                       final ObserveViaCasRequest request) {
    core.send(request);
    return request
      .response()
      .thenApply(response -> {
        if (response.status().success()) {
          boolean found = response.observeStatus() == ObserveViaCasResponse.ObserveStatus.FOUND_PERSISTED
            || response.observeStatus() == ObserveViaCasResponse.ObserveStatus.FOUND_NOT_PERSISTED;
          return new ExistsResult(found, found ? response.cas() : 0);
        }

        final KeyValueErrorContext ctx = KeyValueErrorContext.completedRequest(request, response.status());
        switch (response.status()) {
          case OUT_OF_MEMORY: throw new ServerOutOfMemoryException(ctx);
          case SYNC_WRITE_RE_COMMIT_IN_PROGRESS: throw new DurableWriteReCommitInProgressException(ctx);
          case TEMPORARY_FAILURE: // intended fallthrough to the case below
          case SERVER_BUSY: throw new TemporaryFailureException(ctx);
          default: throw new CouchbaseException("Exists operation failed", ctx);
        }
      })
      .whenComplete((t, e) -> request.context().logicallyComplete());
  }

}

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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.*;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.InsertRequest;

import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.error.DefaultErrorUtil.keyValueStatusToException;
import static com.couchbase.client.java.kv.DurabilityUtils.wrapWithDurability;

@Stability.Internal
public enum InsertAccessor {
  ;

  public static CompletableFuture<MutationResult> insert(final Core core, final InsertRequest request,
                                                         final String key, final PersistTo persistTo,
                                                         final ReplicateTo replicateTo) {
    core.send(request);
    final CompletableFuture<MutationResult> mutationResult = request
      .response()
      .thenApply(response -> {
        if (response.status().success()) {
          return new MutationResult(response.cas(), response.mutationToken());
        } else if (response.status() == ResponseStatus.EXISTS || response.status() == ResponseStatus.NOT_STORED) {
          throw new DocumentExistsException(KeyValueErrorContext.completedRequest(request, response.status()));
        }
        throw keyValueStatusToException(request, response);
      });
    return wrapWithDurability(mutationResult, key, persistTo, replicateTo, core, request, false);
  }

}

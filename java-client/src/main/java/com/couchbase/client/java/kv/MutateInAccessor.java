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
package com.couchbase.client.java.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.java.codec.JsonSerializer;

import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.java.kv.DurabilityUtils.wrapWithDurability;

public class MutateInAccessor {

  public static CompletableFuture<MutateInResult> mutateIn(final Core core, final SubdocMutateRequest request,
                                                           final String key, final PersistTo persistTo,
                                                           final ReplicateTo replicateTo, final Boolean insertDocument,
                                                           final JsonSerializer serializer) {
    core.send(request);
    final CompletableFuture<MutateInResult> mutateInResult = request
      .response()
      .thenApply(response -> {
        if (response.status().success()) {
          return new MutateInResult(response.values(), response.cas(), response.mutationToken(), serializer);
        }
        throw response.throwError(request, insertDocument);
      });
    return wrapWithDurability(mutateInResult, key, persistTo, replicateTo, core, request, false);
  }

}

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
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.DefaultErrorUtil;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.KeyValueErrorContext;
import com.couchbase.client.core.error.subdoc.SubDocumentException;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.java.codec.JsonSerializer;

import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.error.DefaultErrorUtil.keyValueStatusToException;
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

        final KeyValueErrorContext ctx = KeyValueErrorContext.completedRequest(request, response.status());
        if (insertDocument && response.status() == ResponseStatus.EXISTS) {
          throw new DocumentExistsException(ctx);
        } else if (response.status() == ResponseStatus.SUBDOC_FAILURE && response.error().isPresent()) {
          throw response.error().get();
        }
        throw keyValueStatusToException(request, response);
      });
    return wrapWithDurability(mutateInResult, key, persistTo, replicateTo, core, request, false);
  }

}

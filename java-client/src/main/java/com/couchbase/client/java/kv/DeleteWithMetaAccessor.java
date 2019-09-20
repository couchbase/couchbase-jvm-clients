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
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentLockedException;
import com.couchbase.client.core.error.DurabilityAmbiguousException;
import com.couchbase.client.core.error.DurabilityImpossibleException;
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException;
import com.couchbase.client.core.error.DurableWriteInProgressException;
import com.couchbase.client.core.error.DurableWriteReCommitInProgressException;
import com.couchbase.client.core.error.KeyValueErrorContext;
import com.couchbase.client.core.error.ServerOutOfMemoryException;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.DeleteWithMetaRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.Collection;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

public class DeleteWithMetaAccessor {
    private DeleteWithMetaAccessor() {
    }

    public static DeleteWithMetaRequest deleteWithMetaRequest(Collection collection,
                                                              String id,
                                                              byte[] content,
                                                              Duration timeout,
                                                              RetryStrategy retryStrategy,
                                                              long cas, long casToSet,
                                                              long revSeqNoToSet,
                                                              boolean regenerateCas,
                                                              boolean isLastWriteWinsBucket) {
        notNullOrEmpty(id, "Id");
        notNull(collection, "Collection");
        notNull(content, "Content");
        notNull(timeout, "Timeout");
        notNull(retryStrategy, "RetryStrategy");

        CollectionIdentifier collectionIdentifier = new CollectionIdentifier(collection.bucketName(),
                Optional.of(collection.scopeName()),
                Optional.of(collection.name()));

        DeleteWithMetaRequest request = new DeleteWithMetaRequest(id,
                content,
                0,
                regenerateCas,
                timeout,
                collection.core().context(),
                collectionIdentifier,
                retryStrategy,
                cas,
                casToSet,
                revSeqNoToSet,
                isLastWriteWinsBucket);
        return request;
    }

    public static CompletableFuture<MutationResult> deleteWithMeta(final Core core,
                                                            final DeleteWithMetaRequest request,
                                                            final String key) {
        core.send(request);
        return request
                .response()
                .thenApply(response -> {
                    if (response.status().success()) {
                        return new MutationResult(response.cas(), response.mutationToken());
                    }

                    final KeyValueErrorContext ctx = KeyValueErrorContext.completedRequest(request, response.status());
                    switch (response.status()) {
                        case EXISTS: throw new DocumentExistsException(ctx);
                        case LOCKED: throw new DocumentLockedException(ctx);
                        case OUT_OF_MEMORY: throw new ServerOutOfMemoryException(ctx);
                        case TEMPORARY_FAILURE: // intended fallthrough to the case below
                        case SERVER_BUSY: throw new TemporaryFailureException(ctx);
                        case DURABILITY_INVALID_LEVEL: throw new DurabilityLevelNotAvailableException(ctx);
                        case DURABILITY_IMPOSSIBLE: throw new DurabilityImpossibleException(ctx);
                        case SYNC_WRITE_AMBIGUOUS: throw new DurabilityAmbiguousException(ctx);
                        case SYNC_WRITE_IN_PROGRESS: throw new DurableWriteInProgressException(ctx);
                        case SYNC_WRITE_RE_COMMIT_IN_PROGRESS: throw new DurableWriteReCommitInProgressException(ctx);
                        default: throw new CouchbaseException("DeleteWithMeta operation failed", ctx);
                    }
                });
    }
}

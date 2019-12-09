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

package com.couchbase.client.core.error;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.KeyValueRequest;

@Stability.Internal
public class DefaultErrorUtil {
    private DefaultErrorUtil() {
        throw new AssertionError("not instantiable");
    }

    /**
     * Maps common KV response status codes to their corresponding user-level exceptions.
     *
     * @param request the kv request.
     * @param response th response of the kv request.
     * @return the user-level exception from the mapping.
     */
    public static CouchbaseException keyValueStatusToException(final KeyValueRequest<? extends Response> request,
                                                               final Response response) {
        final KeyValueErrorContext ctx = KeyValueErrorContext.completedRequest(request, response.status());
        switch (response.status()) {
            case DURABILITY_INVALID_LEVEL: return new DurabilityLevelNotAvailableException(ctx);
            case DURABILITY_IMPOSSIBLE: return new DurabilityImpossibleException(ctx);
            case EXISTS: return new CasMismatchException(ctx);
            case LOCKED: return new DocumentLockedException(ctx);
            case NOT_FOUND: return new DocumentNotFoundException(ctx);
            case NOT_STORED: return new DocumentNotFoundException(ctx);
            case OUT_OF_MEMORY: return new ServerOutOfMemoryException(ctx);
            case SERVER_BUSY: return new TemporaryFailureException(ctx);
            case SYNC_WRITE_AMBIGUOUS: return new DurabilityAmbiguousException(ctx);
            case SYNC_WRITE_IN_PROGRESS: return new DurableWriteInProgressException(ctx);
            case SYNC_WRITE_RE_COMMIT_IN_PROGRESS: return new DurableWriteReCommitInProgressException(ctx);
            case TEMPORARY_FAILURE: return new TemporaryFailureException(ctx);
            case TOO_BIG: return new ValueTooLargeException(ctx);
            default: return new CouchbaseException(
              request.getClass().getSimpleName() + " failed with unexpected status code",
              ctx
            );
        }
    }

}

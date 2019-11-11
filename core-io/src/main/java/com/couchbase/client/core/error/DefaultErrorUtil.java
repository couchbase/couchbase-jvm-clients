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
import com.couchbase.client.core.msg.ResponseStatus;

@Stability.Internal
public class DefaultErrorUtil {
    private DefaultErrorUtil() {
        throw new AssertionError("not instantiable");
    }

    public static RuntimeException defaultErrorForStatus(String id, ResponseStatus status) {
        // TODO: needs to be removed eventually
        final KeyValueErrorContext errorContext = KeyValueErrorContext.completedRequest(null, status);

        switch(status) {
            case TEMPORARY_FAILURE:
            case SERVER_BUSY:
                return new TemporaryFailureException(errorContext);
            case TOO_BIG: return ValueTooLargeException.forKey(id);
            case EXISTS: return CASMismatchException.forKey(id);
            case NOT_STORED: return DocumentMutationLostException.forKey(id);
            case NOT_FOUND: return new DocumentNotFoundException(errorContext);
            case OUT_OF_MEMORY: return new ServerOutOfMemoryException(errorContext);
            case DURABILITY_INVALID_LEVEL: return new DurabilityLevelNotAvailableException();
            case DURABILITY_IMPOSSIBLE: return new DurabilityImpossibleException();
            case SYNC_WRITE_AMBIGUOUS: return new DurabilityAmbiguousException();
            case SYNC_WRITE_IN_PROGRESS: return new DurableWriteInProgressException();
            case SYNC_WRITE_RE_COMMIT_IN_PROGRESS: return new DurableWriteReCommitInProgressException(errorContext);

                // Any other error should not make it to this generic error handling
                // NOT_MY_VBUCKET - handled at a lower level by retrying the request
                // SUBDOC_FAILURE - handled in the subdoc handling code
                // UNSUPPORTED - probably a client-side bug if this is happening
                // NO_ACCESS - should raise an authentication error at a lower layer
                // UNKNOWN - will drop down to generic CouchbaseException case anyway

            default: return new CouchbaseException("Unknown ResponseStatus", errorContext);
        }
    }

}

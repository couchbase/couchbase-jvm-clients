package com.couchbase.client.core.error;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.*;
import com.couchbase.client.core.msg.ResponseStatus;

@Stability.Internal
public class DefaultErrorUtil {
    private DefaultErrorUtil() { }

    public static RuntimeException defaultErrorForStatus(ResponseStatus status) {
        switch(status) {
            case TOO_BIG: return new RequestTooBigException();
            case EXISTS: return new DocumentAlreadyExistsException();
            case LOCKED: return new TemporaryLockFailureException();
            case TEMPORARY_FAILURE: return new TemporaryFailureException();
            case NOT_STORED: return new DocumentMutationLostException();
            case NOT_FOUND: return new DocumentDoesNotExistException();
            case SERVER_BUSY: return new TemporaryFailureException();
            case OUT_OF_MEMORY: return new CouchbaseOutOfMemoryException();
            case DURABILITY_INVALID_LEVEL: return new DurabilityLevelNotAvailableException();
            case DURABILITY_IMPOSSIBLE: return new DurabilityImpossibleException();
            case SYNC_WRITE_AMBIGUOUS: return new DurabilityAmbiguous();
            case SYNC_WRITE_IN_PROGRESS: return new DurableWriteInProgressException();

                // Any other error should not make it to this generic error handling
                // NOT_MY_VBUCKET - handled at a lower level by retrying the request
                // SUBDOC_FAILURE - handled in the subdoc handling code
                // UNSUPPORTED - probably a client-side bug if this is happening
                // NO_ACCESS - should raise an authentication error at a lower layer
                // UNKNOWN - will drop down to generic CouchbaseException case anyway

            default: return new CouchbaseException("Unknown ResponseStatus: " + status);
        }
    }

}

/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.transaction.error.internal;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.DurabilityAmbiguousException;
import com.couchbase.client.core.error.DurableWriteInProgressException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.ValueTooLargeException;
import com.couchbase.client.core.error.subdoc.PathExistsException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.core.error.transaction.AttemptExpiredException;
import com.couchbase.client.core.error.transaction.DocumentAlreadyInTransactionException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.core.error.transaction.internal.TestFailAmbiguousException;
import com.couchbase.client.core.error.transaction.internal.TestFailHardException;
import com.couchbase.client.core.error.transaction.internal.TestFailTransientException;

@Stability.Internal
public enum ErrorClass {
    // This is a special internal error class indicating the error is already an TransactionOperationFailedException, and usually should be
    // passed through directly
    TRANSACTION_OPERATION_FAILED,

    FAIL_TRANSIENT,
    FAIL_HARD,
    FAIL_OTHER,
    FAIL_AMBIGUOUS,
    FAIL_DOC_ALREADY_EXISTS,
    FAIL_DOC_NOT_FOUND,
    FAIL_PATH_ALREADY_EXISTS,
    FAIL_PATH_NOT_FOUND,
    FAIL_CAS_MISMATCH,
    FAIL_EXPIRY,
    FAIL_WRITE_WRITE_CONFLICT,
    FAIL_ATR_FULL;

    public static ErrorClass classify(Throwable err) {
        if (err instanceof TransactionOperationFailedException) {
            return TRANSACTION_OPERATION_FAILED;
        }
        else if (err instanceof DocumentAlreadyInTransactionException) {
            return FAIL_WRITE_WRITE_CONFLICT;
        }
        else if (err instanceof DocumentNotFoundException) {
            return FAIL_DOC_NOT_FOUND;
        }
        else if (err instanceof DocumentExistsException) {
            return FAIL_DOC_ALREADY_EXISTS;
        }
        else if (err instanceof PathExistsException) {
            return FAIL_PATH_ALREADY_EXISTS;
        }
        else if (err instanceof PathNotFoundException) {
            return FAIL_PATH_NOT_FOUND;
        }
        else if (err instanceof CasMismatchException) {
            return FAIL_CAS_MISMATCH;
        }
        else if (isFailTransient(err)) {
            return FAIL_TRANSIENT;
        }
        else if (isFailAmbiguous(err)) {
            return FAIL_AMBIGUOUS;
        }
        else if (isFailHard(err)) {
            return FAIL_HARD;
        }
        else if (err instanceof AttemptExpiredException) {
            return FAIL_EXPIRY;
        }
        else if (err instanceof ValueTooLargeException) {
            return FAIL_ATR_FULL;
        }
        else {
            return FAIL_OTHER;
        }
    }


    /*
     * What exceptions cause a retry has evolved.  Originally (DP1 and DP2) the rule was to retry on most exceptions.
     * However, this can give a poor user experience.  If the developer has a bug that say raises a NPE, the txn will
     * retry until timeout.  It's confusing.  So as of alpha.3, the rule will be to fast-fail on most exceptions.
     */
    public static boolean isFailTransient(Throwable e) {
        return e instanceof CasMismatchException

                // TXNJ-156: With BestEffortRetryStrategy, many errors such as TempFails will now surface as
                // timeouts instead.  This will include AmbiguousTimeoutException - we should already be able to
                // handle ambiguity, as with DurabilityAmbiguousException
                || e instanceof UnambiguousTimeoutException

                // These only included because several tests explicitly throw them as an error-injection.  Those
                // should be changed to return a more correct TimeoutException.
                || e instanceof TemporaryFailureException
                || e instanceof DurableWriteInProgressException

                // For testing
                || e instanceof TestFailTransientException;
    }

    // For testing, need a mechanism to bypass the usual rollback & error-handling logic and leave a txn in a lost state.
    public static boolean isFailHard(Throwable e) {
        return e instanceof TestFailHardException
                || e instanceof AssertionError;
    }

    public static boolean isFailAmbiguous(Throwable e) {
        return e instanceof DurabilityAmbiguousException
                || e instanceof AmbiguousTimeoutException

                // If a mutation request was cancelled, it may succeed or not
                // TXNJ-163: This is thrown in several situations, including during node failover and removal.
                || e instanceof RequestCanceledException

                // For testing
                || e instanceof TestFailAmbiguousException;
    }
}

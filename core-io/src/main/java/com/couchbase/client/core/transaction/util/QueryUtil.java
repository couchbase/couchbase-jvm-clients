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
package com.couchbase.client.core.transaction.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.context.QueryErrorContext;
import com.couchbase.client.core.error.transaction.AttemptExpiredException;
import com.couchbase.client.core.error.transaction.AttemptNotFoundOnQueryException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;

import java.util.Map;

import static com.couchbase.client.core.error.transaction.TransactionOperationFailedException.Builder.createError;

@Stability.Internal
public class QueryUtil {
    private QueryUtil() {}

    /**
     * Converts raw query error codes into useful exceptions.
     * <p>
     * Once query is returning TransactionOperationFailedException details, this can also raise a TransactionOperationFailedException.
     */
    public static RuntimeException convertQueryError(Throwable err) {
        if (err instanceof TimeoutException) {
            return new AttemptExpiredException(err);
        }
        else if (err instanceof CouchbaseException) {
            // Errors https://issues.couchbase.com/browse/MB-42469
            CouchbaseException ce = (CouchbaseException) err;

            if (ce.context() instanceof QueryErrorContext) {
                QueryErrorContext ctx = (QueryErrorContext) ce.context();
                if (ctx.errors().size() >= 1) {
                    ErrorCodeAndMessage chosenError = chooseQueryError(ctx);
                    int code = chosenError.code();

                    switch (code) {
                        case 1065: // Unknown parameter
                            return createError()
                                    .cause(new FeatureNotAvailableException("Unknown query parameter: note that query support in transactions is available from Couchbase Server 7.0 onwards", err))
                                    .build();
                        case 17004: // Transaction context error
                            return new AttemptNotFoundOnQueryException();
                        case 1080: // Timeout - fall through
                        case 17010: // Transaction timeout
                            return createError()
                                    .cause(new AttemptExpiredException(err))
                                    .doNotRollbackAttempt()
                                    .raiseException(TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_EXPIRED)
                                    .build();
                        case 17012: // Duplicate Key
                            return new DocumentExistsException(ctx);
                        case 17014: // Key not found
                            return new DocumentNotFoundException(ctx);
                        case 17015: // CAS mismatch
                            return new CasMismatchException(ctx);
                    }

                    if (chosenError.context().containsKey("cause")) {
                        // MB-42535
                        Map<String, Object> cause = (Map<String, Object>) chosenError.context().get("cause");

                        Boolean rollbackRaw = (Boolean) cause.get("rollback");
                        Boolean retryRaw = (Boolean) cause.get("retry");
                        String raise = (String) cause.get("raise");

                        // We don't have an errorClass back so always FAIL_OTHER.  This field is going to be removed.
                        TransactionOperationFailedException.Builder builder = createError()
                                .cause(err);

                        switch (raise) {
                            case "failed_post_commit":
                                builder.raiseException(TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT);
                                break;
                            case "commit_ambiguous":
                                builder.raiseException(TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS);
                                break;
                            case "expired":
                                builder.raiseException(TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_EXPIRED);
                                break;
                            case "failed":
                            default:
                                builder.raiseException(TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_FAILED);
                        }

                        if (retryRaw != null && retryRaw) {
                            builder.retryTransaction();
                        }

                        if (rollbackRaw != null && !rollbackRaw) {
                            builder.doNotRollbackAttempt();
                        }

                        return builder.build();
                    }
                }
            }
        }

        return null;
    }

    // TXNJ-400: Query can return multiple errors, we need to heuristically choose the most helpful one
    public static ErrorCodeAndMessage chooseQueryError(QueryErrorContext ctx) {
        // Look for a TransactionOperationFailedException error from gocbcore
        for (ErrorCodeAndMessage err : ctx.errors()) {
            if (err.context().containsKey("cause")) {
                return err;
            }
        }

        // Now for a regular query error containing a transaction error code 17xxx
        for (ErrorCodeAndMessage err : ctx.errors()) {
            if (err.code() >= 17000 && err.code() <= 18000) {
                return err;
            }
        }

        return ctx.errors().get(0);
    }
}

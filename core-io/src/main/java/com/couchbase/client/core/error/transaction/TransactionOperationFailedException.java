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
package com.couchbase.client.core.error.transaction;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.logging.RedactableArgument;

/**
 * This operation (such as a replace, get or insert) either failed or ambiguously succeeded.
 * <p>
 * The details of the failure are opaque, as the application is not expected to take action on this failure.  The
 * application should not catch errors from operations.
 * <p>
 * All methods on this class are for internal use only.
 */
@Stability.Internal
public class TransactionOperationFailedException extends CouchbaseException {
    private final boolean autoRollbackAttempt;
    private final boolean retryTransaction;

    @Stability.Internal
    public enum
    FinalErrorToRaise {
        TRANSACTION_SUCCESS,              // 0b000
        TRANSACTION_FAILED,               // 0b001
        TRANSACTION_EXPIRED,              // 0b010
        TRANSACTION_COMMIT_AMBIGUOUS,     // 0b011

        /**
         * This will currently result in returning success to the application, but unstagingCompleted() will be false.
         */
        TRANSACTION_FAILED_POST_COMMIT    // 0b100
    }

    // The exception to raise after all error handling is done
    private final FinalErrorToRaise toRaise;

    public TransactionOperationFailedException(boolean autoRollbackAttempt,
                                               boolean retryTransaction,
                                               Throwable cause,
                                               FinalErrorToRaise toRaise) {
        super("Internal transaction error", cause);
        this.autoRollbackAttempt = autoRollbackAttempt;
        this.retryTransaction = retryTransaction;
        this.toRaise = toRaise;
    }

    @Stability.Internal
    public boolean autoRollbackAttempt() {
        return autoRollbackAttempt;
    }

    @Stability.Internal
    public boolean retryTransaction() {
        return retryTransaction;
    }

    @Stability.Internal
    public FinalErrorToRaise toRaise() {
        return toRaise;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TransactionOperationFailedException {");
        if (getCause() != null) {
            sb.append("cause:");
            sb.append(RedactableArgument.redactUser(getCause().getClass().getName()));
        }
        sb.append(", retry:");
        sb.append(retryTransaction);
        sb.append(", autoRollback:");
        sb.append(autoRollbackAttempt);
        if (!retryTransaction) {
            sb.append(", raise:");
            sb.append(toRaise);
        }
        sb.append("}");
        return sb.toString();
    }

    public static class Builder {
        private boolean rollbackAttempt = true;
        private boolean retryTransaction = false;
        private Throwable cause = null;
        private TransactionOperationFailedException.FinalErrorToRaise toRaise = TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_FAILED;

        private Builder() {
        }

        public static Builder createError() {
            return new Builder();
        }

        public Builder raiseException(TransactionOperationFailedException.FinalErrorToRaise toRaise) {
            this.toRaise = toRaise;
            return this;
        }

        public Builder doNotRollbackAttempt() {
            rollbackAttempt = false;
            return this;
        }

        public Builder rollbackAttempt(boolean value) {
            rollbackAttempt = value;
            return this;
        }

        public Builder retryTransaction() {
            retryTransaction = true;
            return this;
        }

        public Builder cause(Throwable cause) {
            this.cause = cause;
            return this;
        }

        public TransactionOperationFailedException build() {
            return new TransactionOperationFailedException(rollbackAttempt,
                    retryTransaction,
                    cause,
                    toRaise);
        }
    }
}


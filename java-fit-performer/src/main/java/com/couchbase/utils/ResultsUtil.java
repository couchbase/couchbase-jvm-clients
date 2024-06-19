/*
 * Copyright (c) 2020 Couchbase, Inc.
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
// [skip:<3.3.0]
package com.couchbase.utils;

import com.couchbase.InternalPerformerFailure;
import com.couchbase.JavaPerformer;
import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.ParsingFailureException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.transaction.ActiveTransactionRecordEntryNotFoundException;
import com.couchbase.client.core.error.transaction.ActiveTransactionRecordFullException;
import com.couchbase.client.core.error.transaction.ActiveTransactionRecordNotFoundException;
import com.couchbase.client.core.error.transaction.CommitNotPermittedException;
import com.couchbase.client.core.error.transaction.ConcurrentOperationsDetectedOnSameDocumentException;
import com.couchbase.client.core.error.transaction.ForwardCompatibilityFailureException;
import com.couchbase.client.core.error.transaction.PreviousOperationFailedException;
import com.couchbase.client.core.error.transaction.RollbackNotPermittedException;
import com.couchbase.client.core.error.transaction.TransactionAlreadyAbortedException;
import com.couchbase.client.core.error.transaction.TransactionAlreadyCommittedException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.core.error.transaction.DocumentAlreadyInTransactionException;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordEntry;
import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupAttemptEvent;
import com.couchbase.client.core.transaction.support.AttemptState;
import com.couchbase.client.java.transactions.error.TransactionCommitAmbiguousException;
import com.couchbase.client.java.transactions.error.TransactionExpiredException;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import com.couchbase.client.protocol.transactions.DocId;
import com.couchbase.client.protocol.transactions.ExternalException;
import com.couchbase.client.protocol.transactions.TransactionException;
import com.couchbase.client.protocol.transactions.TransactionResult;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ResultsUtil {
    private ResultsUtil() {}

    public static TransactionResult createResult(Optional<Exception> exception,
                                                 com.couchbase.client.java.transactions.TransactionResult transactionResult ,
                                                 Optional<Integer> cleanupRequests) {
        return createResult(exception,
                transactionResult.logs(),
                transactionResult.transactionId(),
                transactionResult.unstagingComplete(),
                cleanupRequests);
    }

    public static TransactionResult createResult(Optional<Exception> exception,
                                                 @Nullable List<TransactionLogEvent> logs,
                                                 String transactionId,
                                                 boolean unstagingComplete,
                                                 Optional<Integer> cleanupRequests) {
        TransactionResult.Builder response =
            TransactionResult.getDefaultInstance().newBuilderForType();

        exception.ifPresent(ex -> {
            response.setException(convertTransactionFailed(ex));
            response.setExceptionCause(mapCause(ex.getCause()));
        });

        if (!exception.isPresent()) {
            response.setException(TransactionException.NO_EXCEPTION_THROWN);
        }

        if (cleanupRequests.isPresent()) {
            response.setCleanupRequestsValid(true);
            response.setCleanupRequestsPending(cleanupRequests.get());
        }
        else {
            response.setCleanupRequestsValid(false);
        }

        response.setTransactionId(transactionId);
        response.setUnstagingComplete(unstagingComplete);

        if (logs != null) {
            logs.forEach(l ->
                    response.addLog(l.toString()));
        }

        String globalError = JavaPerformer.globalError.getAndSet(null);
        if (globalError != null) {
            response.setPerformerSpecificValidation(globalError);
        }

        return response.build();
    }

    public static TransactionException convertTransactionFailed(Exception ex) {
        if (ex instanceof TransactionExpiredException) {
            return TransactionException.EXCEPTION_EXPIRED;
        } else if (ex instanceof TransactionCommitAmbiguousException) {
            return TransactionException.EXCEPTION_COMMIT_AMBIGUOUS;
        } else if (ex instanceof TransactionFailedException) {
            return TransactionException.EXCEPTION_FAILED;
        } else {
            return TransactionException.EXCEPTION_UNKNOWN;
        }
    }

    public static com.couchbase.client.protocol.transactions.AttemptStates mapState(AttemptState state) {
        switch (state) {
            case ABORTED:
                return com.couchbase.client.protocol.transactions.AttemptStates.ABORTED;
            case COMMITTED:
                return com.couchbase.client.protocol.transactions.AttemptStates.COMMITTED;
            case NOT_STARTED:
                // NOTHING_WRITTEN is a better name than NOT_STARTED, as it could in fact be a completed
                // read-only transaction, but the enum has been published now.
                return com.couchbase.client.protocol.transactions.AttemptStates.NOTHING_WRITTEN;
            case COMPLETED:
                return com.couchbase.client.protocol.transactions.AttemptStates.COMPLETED;
            case PENDING:
                return com.couchbase.client.protocol.transactions.AttemptStates.PENDING;
            case ROLLED_BACK:
                return com.couchbase.client.protocol.transactions.AttemptStates.ROLLED_BACK;
            default:
                return com.couchbase.client.protocol.transactions.AttemptStates.UNKNOWN;
        }
    }

    public static ExternalException mapCause(Throwable ex) {
        if (ex == null) {
            return ExternalException.NotSet;
        }
        else if (ex instanceof ActiveTransactionRecordEntryNotFoundException) {
            return ExternalException.ActiveTransactionRecordEntryNotFound;
        }
        else if (ex instanceof ActiveTransactionRecordFullException) {

            return ExternalException.ActiveTransactionRecordFull;
        }
        else if (ex instanceof ActiveTransactionRecordNotFoundException) {
            return ExternalException.ActiveTransactionRecordNotFound;
        }
        else if (ex instanceof DocumentAlreadyInTransactionException) {
            return ExternalException.DocumentAlreadyInTransaction;
        }
        else if (ex instanceof DocumentExistsException) {
            return ExternalException.DocumentExistsException;
        }
        else if (ex instanceof DocumentNotFoundException) {
            return ExternalException.DocumentNotFoundException;
        }
        else if (ex instanceof FeatureNotAvailableException) {
            return ExternalException.FeatureNotAvailableException;
        }
        else if (ex instanceof PreviousOperationFailedException) {
            return ExternalException.PreviousOperationFailed;
        }
        else if (ex instanceof ForwardCompatibilityFailureException) {
            return ExternalException.ForwardCompatibilityFailure;
        }
        else if (ex instanceof ParsingFailureException) {
            return ExternalException.ParsingFailure;
        }
        else if (ex instanceof IllegalStateException) {
            return ExternalException.IllegalStateException;
        }
        else if (ex instanceof com.couchbase.client.core.error.ServiceNotAvailableException) {
            return ExternalException.ServiceNotAvailableException;
        }
        else if (ex instanceof ConcurrentOperationsDetectedOnSameDocumentException) {
            return ExternalException.ConcurrentOperationsDetectedOnSameDocument;
        }
        else if (ex instanceof CommitNotPermittedException) {
            return ExternalException.CommitNotPermitted;
        }
        else if (ex instanceof RollbackNotPermittedException) {
            return ExternalException.RollbackNotPermitted;
        }
        else if (ex instanceof TransactionAlreadyAbortedException) {
            return ExternalException.TransactionAlreadyAborted;
        }
        else if (ex instanceof TransactionAlreadyCommittedException) {
            return ExternalException.TransactionAlreadyCommitted;
        }
        else if (ex instanceof UnambiguousTimeoutException) {
            return ExternalException.UnambiguousTimeoutException;
        }
        else if (ex instanceof AmbiguousTimeoutException) {
            return ExternalException.AmbiguousTimeoutException;
        }
        else if (ex instanceof AuthenticationFailureException) {
            return ExternalException.AuthenticationFailureException;
        }
        else if (ex instanceof com.couchbase.client.core.error.CouchbaseException) {
            return ExternalException.CouchbaseException;
        }
        else {
            return ExternalException.Unknown;
        }
    }

    public static com.couchbase.client.protocol.transactions.TransactionCleanupAttempt mapCleanupAttempt(TransactionCleanupAttemptEvent result,
                                                                                          Optional<ActiveTransactionRecordEntry> atrEntry) {

        var builder = com.couchbase.client.protocol.transactions.TransactionCleanupAttempt.newBuilder()
                .setSuccess(result.success())
                .setAttemptId(result.attemptId())
                .addAllLogs(result.logs().stream().map(TransactionLogEvent::toString).collect(Collectors.toList()))
                .setAtr(DocId.newBuilder()
                        .setBucketName(result.atrCollection().bucket())
                        .setScopeName(result.atrCollection().scope().get())
                        .setCollectionName(result.atrCollection().collection().get())
                        .setDocId(result.atrId())
                        .build());

        atrEntry.ifPresent(ae ->
                builder.setState(mapState(ae.state())));

        return builder.build();
    }

    public static TransactionException mapToRaise(TransactionOperationFailedException.FinalErrorToRaise toRaise) {
        switch (toRaise) {
            case TRANSACTION_FAILED:
                return TransactionException.EXCEPTION_FAILED;
            case TRANSACTION_EXPIRED:
                return TransactionException.EXCEPTION_EXPIRED;
            case TRANSACTION_COMMIT_AMBIGUOUS:
                return TransactionException.EXCEPTION_COMMIT_AMBIGUOUS;
            case TRANSACTION_FAILED_POST_COMMIT:
                return TransactionException.EXCEPTION_FAILED_POST_COMMIT;
            default:
                throw new InternalPerformerFailure(
                        new IllegalArgumentException("Unknown toRaise " + toRaise));
        }
    }
}

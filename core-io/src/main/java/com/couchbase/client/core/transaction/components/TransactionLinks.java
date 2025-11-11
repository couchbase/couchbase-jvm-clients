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

package com.couchbase.client.core.transaction.components;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreExpiry;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.forwards.ForwardCompatibility;

import java.util.Objects;
import java.util.Optional;

/**
 * Stores the transaction links stored on each document in a transaction
 */
@Stability.Internal
public class TransactionLinks {
    private final Optional<String> atrId;
    private final Optional<String> atrBucketName;
    private final Optional<String> atrScopeName;
    private final Optional<String> atrCollectionName;
    // ID of the transaction that has staged content
    private final Optional<String> stagedTransactionId;
    private final Optional<String> stagedAttemptId;
    private final Optional<byte[]> stagedContentJson;
    private final Optional<byte[]> stagedContentBinary;
    private final Optional<String> crc32OfStaging;

    // For {BACKUP_FIELDS}
    private final Optional<String> casPreTxn;
    private final Optional<String> revidPreTxn;
    private final Optional<Long> exptimePreTxn;
    private final Optional<String> op;

    private final Optional<ForwardCompatibility> forwardCompatibility;
    private final Optional<String> stagedOperationId;
    private final Optional<Integer> stagedUserFlags;
    private final Optional<CoreExpiry> stagedExpiry;

    /**
     * This is not part of the transactional metadata.  It's here for legacy reasons and could be refactoring into
     * TransactionGetResultInternal.
     */
    private final boolean isDeleted;

    public TransactionLinks(
            Optional<byte[]> stagedContentJson,
            Optional<byte[]> stagedContentBinary,
            Optional<String> atrId,
            Optional<String> atrBucketName,
            Optional<String> atrScopeName,
            Optional<String> atrCollectionName,
            Optional<String> stagedTransactionId,
            Optional<String> stagedAttemptId,
            Optional<String> casPreTxn,
            Optional<String> revidPreTxn,
            Optional<Long> exptimePreTxn,
            Optional<String> op,
            boolean isDeleted,
            Optional<String> crc32OfStaging,
            Optional<ForwardCompatibility> forwardCompatibility,
            Optional<String> stagedOperationId,
            Optional<Integer> stagedUserFlags,
            Optional<CoreExpiry> stagedExpiry) {
        this.stagedContentJson = Objects.requireNonNull(stagedContentJson);
        this.stagedContentBinary = Objects.requireNonNull(stagedContentBinary);
        this.atrId = Objects.requireNonNull(atrId);
        this.stagedTransactionId = Objects.requireNonNull(stagedTransactionId);
        this.stagedAttemptId = Objects.requireNonNull(stagedAttemptId);
        this.atrBucketName = Objects.requireNonNull(atrBucketName);
        this.atrScopeName = Objects.requireNonNull(atrScopeName);
        this.atrCollectionName = Objects.requireNonNull(atrCollectionName);
        this.casPreTxn = Objects.requireNonNull(casPreTxn);
        this.revidPreTxn = Objects.requireNonNull(revidPreTxn);
        this.exptimePreTxn = Objects.requireNonNull(exptimePreTxn);
        this.op = Objects.requireNonNull(op);
        this.isDeleted = isDeleted;
        this.crc32OfStaging = Objects.requireNonNull(crc32OfStaging);
        this.forwardCompatibility = Objects.requireNonNull(forwardCompatibility);
        this.stagedOperationId = Objects.requireNonNull(stagedOperationId);
        this.stagedUserFlags = stagedUserFlags;
        this.stagedExpiry = Objects.requireNonNull(stagedExpiry);
    }

    /**
     * Note this doesn't guarantee an active transaction, as it may have expired and need rolling back.
     */
    public boolean isDocumentInTransaction() {
        return atrId.isPresent();
    }

    public boolean isDocumentBeingRemoved() {
        return op.isPresent() && op.get().equals(OperationTypes.REMOVE);
    }

    public boolean hasStagedWrite() {
        return stagedAttemptId.isPresent();
    }

    public Optional<String> atrId() {
        return atrId;
    }

    public Optional<String> stagedTransactionId() {
        return stagedTransactionId;
    }


    public Optional<String> stagedAttemptId() {
        return stagedAttemptId;
    }

    @Stability.Internal
    public Optional<byte[]> stagedContentJsonOrBinary() {
      if (stagedContentJson.isPresent()) {
        return stagedContentJson;
      }
      return stagedContentBinary;
    }

    @Stability.Internal
    public Optional<byte[]> stagedContentBinary() {
      return stagedContentBinary;
    }

    @Stability.Internal
    public Optional<byte[]> stagedContentJson() {
      return stagedContentJson;
    }

    public Optional<String> atrBucketName() {
        return atrBucketName;
    }

    public Optional<String> atrScopeName() {
        return atrScopeName;
    }

    public Optional<String> atrCollectionName() {
        return atrCollectionName;
    }

    public Optional<String> casPreTxn() {
        return casPreTxn;
    }

    public Optional<String> revidPreTxn() {
        return revidPreTxn;
    }

    public Optional<Long> exptimePreTxn() {
        return exptimePreTxn;
    }

    public Optional<String> op() {
        return op;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    /**
     * The CRC32 from staging the document.
     * <p>
     * It is only available if it has been explicitly fetched.  E.g. it will not be present after the mutation (which
     * cannot return this field).
     */
    public Optional<String> crc32OfStaging() {
        return crc32OfStaging;
    }

    public Optional<ForwardCompatibility> forwardCompatibility() {
        return forwardCompatibility;
    }

    public Optional<String> stagedOperationId() {
        return stagedOperationId;
    }

    public Optional<Integer> stagedUserFlags() {
        return stagedUserFlags;
    }

    public Optional<CoreExpiry> stagedExpiry() {
        return stagedExpiry;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TransactionLinks{");
        sb.append("atr=");
        sb.append(atrBucketName.orElse("none")).append('.');
        sb.append(atrScopeName.orElse("none")).append('.');
        sb.append(atrCollectionName.orElse("none")).append('.');
        sb.append(atrId.orElse("none")).append('.');
        sb.append(",txnId=").append(stagedTransactionId.orElse("none"));
        sb.append(",attemptId=").append(stagedAttemptId.orElse("none"));
        sb.append(",crc32Staging=").append(crc32OfStaging.orElse("none"));
        sb.append(",isDeleted=").append(isDeleted);
        stagedContentJson.ifPresent(s ->
                sb.append(",content=").append(s.length).append("bytes"));
        sb.append(",op=").append(op.orElse("none"));
        sb.append(",fc=").append(forwardCompatibility.map(Object::toString).orElse("none"));
        sb.append(",opId=").append(stagedOperationId.orElse("none"));
        sb.append(",binary=").append(stagedContentBinary.isPresent());
        sb.append(",userFlags=").append(stagedUserFlags.map(Object::toString).orElse("none"));
        sb.append(",restore={");
        sb.append(casPreTxn.orElse("none"));
        sb.append(',');
        sb.append(revidPreTxn.orElse("none"));
        sb.append(',');
        sb.append(exptimePreTxn.orElse(-1L));
        sb.append(',');
        sb.append(stagedExpiry.map(CoreExpiry::toString).orElse("-"));
        sb.append("}}");
        return sb.toString();
    }

    public CollectionIdentifier collection() {
        return new CollectionIdentifier(atrBucketName.get(), atrScopeName, atrCollectionName);
    }
}

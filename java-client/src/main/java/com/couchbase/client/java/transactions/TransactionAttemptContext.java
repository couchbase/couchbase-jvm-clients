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

package com.couchbase.client.java.transactions;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.query.CoreQueryContext;
import com.couchbase.client.core.api.query.CoreQueryOptions;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.transactions.config.TransactionGetOptions;
import com.couchbase.client.java.transactions.config.TransactionGetReplicaFromPreferredServerGroupOptions;
import com.couchbase.client.java.transactions.config.TransactionInsertOptions;
import com.couchbase.client.java.transactions.config.TransactionReplaceOptions;
import com.couchbase.client.java.transactions.getmulti.TransactionGetMultiOptions;
import com.couchbase.client.java.transactions.getmulti.TransactionGetMultiReplicasFromPreferredServerGroupSpec;
import com.couchbase.client.java.transactions.getmulti.TransactionGetMultiReplicasFromPreferredServerGroupOptions;
import com.couchbase.client.java.transactions.getmulti.TransactionGetMultiReplicasFromPreferredServerGroupResult;
import com.couchbase.client.java.transactions.getmulti.TransactionGetMultiResult;
import com.couchbase.client.java.transactions.getmulti.TransactionGetMultiSpec;
import com.couchbase.client.java.transactions.getmulti.TransactionGetMultiUtil;

import java.util.List;
import java.util.Objects;

import static com.couchbase.client.core.cnc.TracingIdentifiers.TRANSACTION_OP_INSERT;
import static com.couchbase.client.core.cnc.TracingIdentifiers.TRANSACTION_OP_REMOVE;
import static com.couchbase.client.core.cnc.TracingIdentifiers.TRANSACTION_OP_REPLACE;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.java.transactions.config.TransactionGetOptions.transactionGetOptions;
import static com.couchbase.client.java.transactions.config.TransactionGetReplicaFromPreferredServerGroupOptions.transactionGetReplicaFromPreferredServerGroupOptions;
import static com.couchbase.client.java.transactions.config.TransactionInsertOptions.transactionInsertOptions;
import static com.couchbase.client.java.transactions.config.TransactionReplaceOptions.transactionReplaceOptions;
import static com.couchbase.client.java.transactions.getmulti.TransactionGetMultiOptions.transactionGetMultiOptions;
import static com.couchbase.client.java.transactions.getmulti.TransactionGetMultiReplicasFromPreferredServerGroupOptions.transactionGetMultiReplicasFromPreferredServerGroupOptions;
import static com.couchbase.client.java.transactions.internal.ConverterUtil.makeCollectionIdentifier;
import static com.couchbase.client.java.transactions.internal.EncodingUtil.encode;

/**
 * Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents, as well
 * as
 * commit or rollback the transaction.
 * <p>
 * These methods are blocking/synchronous.  See {@link CoreTransactionAttemptContext} for the asynchronous version (which is
 * the
 * preferred option, and which this class largely simply wraps).
 */
public class TransactionAttemptContext {
    private static final TransactionGetOptions DEFAULT_GET_OPTIONS = transactionGetOptions();
    private static final TransactionInsertOptions DEFAULT_INSERT_OPTIONS = transactionInsertOptions();
    private static final TransactionReplaceOptions DEFAULT_REPLACE_OPTIONS = transactionReplaceOptions();
    private static final TransactionGetReplicaFromPreferredServerGroupOptions DEFAULT_GET_REPLICA_OPTIONS = transactionGetReplicaFromPreferredServerGroupOptions();
    private static final TransactionGetMultiOptions DEFAULT_GET_MULTI_OPTIONS = transactionGetMultiOptions();
    private static final TransactionGetMultiReplicasFromPreferredServerGroupOptions DEFAULT_GET_MULTI_REPLICA_OPTIONS = transactionGetMultiReplicasFromPreferredServerGroupOptions();

    private final CoreTransactionAttemptContext internal;
    private final JsonSerializer serializer;

    TransactionAttemptContext(CoreTransactionAttemptContext internal, JsonSerializer serializer) {
        this.internal = Objects.requireNonNull(internal);
        this.serializer = Objects.requireNonNull(serializer);
    }

    @Stability.Internal
    CoreTransactionAttemptContext ctx() {
        return internal;
    }

    @SuppressWarnings("unused")
    @Stability.Internal
    CoreTransactionLogger logger() {
        return ctx().logger();
    }

    /**
     * Gets a document from the specified Couchbase <code>collection</code> matching the specified <code>id</code>.  If
     * the document is not found, a <code>DocumentNotFoundException</code> is thrown.
     *
     * @param collection the Couchbase collection the document exists on
     * @param id         the document's ID
     * @return a <code>TransactionGetResult</code> containing the document
     */
    public TransactionGetResult get(Collection collection, String id) {
        return get(collection, id, DEFAULT_GET_OPTIONS);
    }

    /**
     * Gets a document from the specified Couchbase <code>collection</code> matching the specified <code>id</code>.  If
     * the document is not found, a <code>DocumentNotFoundException</code> is thrown.
     *
     * @param collection the Couchbase collection the document exists on
     * @param id         the document's ID
     * @param options    options controlling the operation
     * @return a <code>TransactionGetResult</code> containing the document
     */
    public TransactionGetResult get(Collection collection, String id, TransactionGetOptions options) {
        notNull(options, "Options");
        TransactionGetOptions.Built built = options.build();
        return internal.get(makeCollectionIdentifier(collection.async()), id)
            .map(result -> new TransactionGetResult(result, serializer(), built.transcoder()))
            .block();
    }

    /**
     * A convenience wrapper around {@link #getReplicaFromPreferredServerGroup(Collection, String, TransactionGetReplicaFromPreferredServerGroupOptions)}
     * using default options.
     */
    public TransactionGetResult getReplicaFromPreferredServerGroup(Collection collection, String id) {
        return getReplicaFromPreferredServerGroup(collection, id, DEFAULT_GET_REPLICA_OPTIONS);
    }

    /**
     * Gets a document from the specified Couchbase <code>collection</code> matching the specified <code>id</code>.
     * <p>
     * It will be fetched only from document copies that on nodes in the preferred server group, which can
     * be configured with {@link com.couchbase.client.java.env.ClusterEnvironment.Builder#preferredServerGroup(String)}.
     * <p>
     * If no replica can be retrieved, which can include for reasons such as this preferredServerGroup not being set,
     * and misconfigured server groups, then {@link com.couchbase.client.core.error.DocumentUnretrievableException}
     * can be raised.  It is strongly recommended that this method always be used with a fallback strategy, such as:
     * <code>
     * try {
     *   var result = ctx.getReplicaFromPreferredServerGroup(collection, id);
     * } catch (DocumentUnretrievableException err) {
     *   var result = ctx.get(collection, id);
     * }
     * </code>
     *
     * @param collection the Couchbase collection the document exists on
     * @param id         the document's ID
     * @param options    options controlling the operation
     * @return a <code>TransactionGetResult</code> containing the document
     */
    public TransactionGetResult getReplicaFromPreferredServerGroup(Collection collection, String id, TransactionGetReplicaFromPreferredServerGroupOptions options) {
        notNull(options, "Options");
        TransactionGetReplicaFromPreferredServerGroupOptions.Built built = options.build();
        return internal.getReplicaFromPreferredServerGroup(makeCollectionIdentifier(collection.async()), id)
            .map(result -> new TransactionGetResult(result, serializer(), built.transcoder()))
            .block();
    }

    /**
     * A convenience wrapper around {@link #getMulti(List, TransactionGetMultiOptions)} using default options.
     */
    @Stability.Uncommitted
    public TransactionGetMultiResult getMulti(List<TransactionGetMultiSpec> specs) {
        return getMulti(specs, DEFAULT_GET_MULTI_OPTIONS);
    }

    /**
     * Fetches multiple documents in a single operation.
     * <p>
     * In addition, it will heuristically aim to detect read skew anomalies, and avoid them if possible.  Read skew detection and avoidance is not guaranteed.
     * <p>
     * @param specs the documents to fetch.
     * @return a result containing the fetched documents.
     */
    @Stability.Uncommitted
    public TransactionGetMultiResult getMulti(List<TransactionGetMultiSpec> specs, TransactionGetMultiOptions options) {
        notNull(options, "options");
        return internal.getMultiAlgo(TransactionGetMultiUtil.convert(specs), options.build(), false)
            .map(result -> TransactionGetMultiUtil.convert(result, specs, serializer()))
            .block();
    }

    /**
     * A convenience wrapper around {@link #getMulti(List, TransactionGetMultiOptions)} using default options.
     */
    @Stability.Uncommitted
    public TransactionGetMultiReplicasFromPreferredServerGroupResult getMultiReplicasFromPreferredServerGroup(List<TransactionGetMultiReplicasFromPreferredServerGroupSpec> specs) {
        return getMultiReplicasFromPreferredServerGroup(specs, DEFAULT_GET_MULTI_REPLICA_OPTIONS);
    }

    /**
     * Similar to {@link #getMulti(List, TransactionGetMultiOptions)}, but fetches the documents from replicas in the preferred server group.
     * <p>
     * Note that the nature of replicas is that they are eventually consistent with the active, and so the effectiveness of read skew detection may be impacted.
     */
    @Stability.Uncommitted
    public TransactionGetMultiReplicasFromPreferredServerGroupResult getMultiReplicasFromPreferredServerGroup(List<TransactionGetMultiReplicasFromPreferredServerGroupSpec> specs, TransactionGetMultiReplicasFromPreferredServerGroupOptions options) {
        notNull(options, "options");
        return internal.getMultiAlgo(TransactionGetMultiUtil.convertReplica(specs), options.build(), true)
            .map(result -> TransactionGetMultiUtil.convertReplica(result, specs, serializer()))
            .block();
    }

    /**
     * Mutates the specified <code>doc</code> with new content.
     * <p>
     * The mutation is staged until the transaction is committed.  That is, any read of the document by any Couchbase
     * component will see the document's current value, rather than this staged or 'dirty' data.  If the attempt is
     * rolled back, the staged mutation will be removed.
     * <p>
     * This staged data effectively locks the document from other transactional writes until the attempt completes
     * (commits or rolls back).
     * <p>
     * If the mutation fails with a <code>CasMismatchException</code>, or any other exception, the transaction will
     * automatically
     * rollback this attempt, then retry.
     *
     * @param doc     the doc to be updated
     * @param content the content to replace the doc with.  This will normally be a {@link JsonObject}.
     * @return the doc, updated with its new CAS value.  For performance a copy is not created and the original doc
     * object is modified.
     */
    public TransactionGetResult replace(TransactionGetResult doc, Object content) {
        return replace(doc, content, DEFAULT_REPLACE_OPTIONS);
    }

    /**
     * Mutates the specified <code>doc</code> with new content.
     * <p>
     * The mutation is staged until the transaction is committed.  That is, any read of the document by any Couchbase
     * component will see the document's current value, rather than this staged or 'dirty' data.  If the attempt is
     * rolled back, the staged mutation will be removed.
     * <p>
     * This staged data effectively locks the document from other transactional writes until the attempt completes
     * (commits or rolls back).
     * <p>
     * If the mutation fails with a <code>CasMismatchException</code>, or any other exception, the transaction will
     * automatically rollback this attempt, then retry.
     *
     * @param doc     the doc to be updated
     * @param content the content to replace the doc with.  This will normally be a {@link JsonObject}.
     * @param options options controlling the operation
     * @return the doc, updated with its new CAS value.  For performance a copy is not created and the original doc
     * object is modified.
     */
    public TransactionGetResult replace(TransactionGetResult doc, Object content, TransactionReplaceOptions options) {
        notNull(options, "Options");
        TransactionReplaceOptions.Built built = options.build();
        RequestSpan span = CbTracing.newSpan(internal.core().context(), TRANSACTION_OP_REPLACE, internal.span());
        Transcoder.EncodedValue encoded = encode(content, span, serializer, built.transcoder(), internal.core().context());
        return internal.replace(doc.internal(), encoded.encoded(), encoded.flags(), built.expiry(), new SpanWrapper(span))
            .map(result -> new TransactionGetResult(result, serializer(), built.transcoder()))
            .doOnError(err -> span.status(RequestSpan.StatusCode.ERROR))
            .doOnTerminate(() -> span.end())
            .block();
    }

    private JsonSerializer serializer() {
        return serializer;
    }

    /**
     * Inserts a new document into the specified Couchbase <code>collection</code>.
     * <p>
     * As with {@link #replace}, the insert is staged until the transaction is committed.
     * <p>
     * This staged data effectively locks the document from other transactional writes until the attempt completes
     * (commits or rolls back).
     *
     * @param collection the Couchbase collection in which to insert the doc
     * @param id         the document's unique ID
     * @param content    the content to insert.  Generally this will be a
     *                   {@link com.couchbase.client.java.json.JsonObject}
     * @return the doc, updated with its new CAS value and ID, and converted to a <code>TransactionGetResult</code>
     */
    public TransactionGetResult insert(Collection collection, String id, Object content) {
        return insert(collection, id, content, DEFAULT_INSERT_OPTIONS);
    }

    /**
     * Inserts a new document into the specified Couchbase <code>collection</code>.
     * <p>
     * As with {@link #replace}, the insert is staged until the transaction is committed.
     * <p>
     * This staged data effectively locks the document from other transactional writes until the attempt completes
     * (commits or rolls back).
     *
     * @param collection the Couchbase collection in which to insert the doc
     * @param id         the document's unique ID
     * @param content    the content to insert.  Generally this will be a
     *                   {@link com.couchbase.client.java.json.JsonObject}
     * @param options    options controlling the operation
     * @return the doc, updated with its new CAS value and ID, and converted to a <code>TransactionGetResult</code>
     */
    public TransactionGetResult insert(Collection collection, String id, Object content, TransactionInsertOptions options) {
        notNull(options, "Options");
        TransactionInsertOptions.Built built = options.build();
        RequestSpan span = CbTracing.newSpan(internal.core().context(), TRANSACTION_OP_INSERT, internal.span());
        Transcoder.EncodedValue encoded = encode(content, span, serializer, built.transcoder(), internal.core().context());
        return internal.insert(makeCollectionIdentifier(collection.async()), id, encoded.encoded(), encoded.flags(), built.expiry(), new SpanWrapper(span))
            .map(result -> new TransactionGetResult(result, serializer(), built.transcoder()))
            .doOnError(err -> span.status(RequestSpan.StatusCode.ERROR))
            .doOnTerminate(() -> span.end())
            .block();
    }

    /**
     * Removes the specified <code>doc</code>.
     * <p>
     * As with {@link #replace}, the remove is staged until the transaction is committed.  That is, the document will
     * continue to exist, and the rest of the Couchbase platform will continue to see it.
     * <p>
     * This staged data effectively locks the document from other transactional writes until the attempt completes
     * (commits or rolls back).
     * <p>
     * Note that a <code>remove(String id)</code> method is not possible, as it's necessary to check a provided
     * <code>TransactionGetResult</code> to determine if the document is involved in another transaction.
     *
     * @param doc the doc to be removed
     */
    public void remove(TransactionGetResult doc) {
        RequestSpan span = CbTracing.newSpan(internal.core().context(), TRANSACTION_OP_REMOVE, internal.span());
        internal.remove(doc.internal(), new SpanWrapper(span))
                .doOnError(err -> span.status(RequestSpan.StatusCode.ERROR))
                .doOnTerminate(() -> span.end())
                .block();
    }

    /**
     * Runs a N1QL query and returns the result.
     * <p>
     * All rows are buffered in-memory.
     * <p>
     * @throws CouchbaseException or an error derived from it on failure.  The application can choose to catch and ignore this error, and the
     *       transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will
     *       cause the attempt to fail.
     */
    public TransactionQueryResult query(String statement,
                             TransactionQueryOptions options) {
        return query(null, statement, options);
    }

    /**
     * Calls query() with default options.
     */
    public TransactionQueryResult query(String statement) {
        return query(null, statement, null);
    }

    /**
     * Runs a N1QL query and returns the result.
     * <p>
     * All rows are buffered in-memory.
     * <p>
     * This overload performs a 'scope-level query': that is, one in which a collection may be referenced by name in the
     * query statement, without needing to specify the full bucket.scope.collection syntax.
     * <p>
     * @throws CouchbaseException or an error derived from it on failure.  The application can choose to catch and ignore this error, and the
     *       transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will
     *       cause the attempt to fail.
     */
    public TransactionQueryResult query(Scope scope,
                             String statement,
                             TransactionQueryOptions options) {
        CoreQueryOptions opts = options != null ? options.builder().build() : null;
        return internal.queryBlocking(statement,
                        scope == null ? null : CoreQueryContext.of(scope.bucketName(), scope.name()),
                        opts,
                        false)
                .publishOn(internal.core().context().environment().transactionsSchedulers().schedulerBlocking())
                .map(response -> new TransactionQueryResult(response, serializer()))
                .block();
    }

    /**
     * Calls query() with default options.
     * <p>
     * This overload performs a 'scope-level query': that is, one in which a collection may be referenced by name in the
     * query statement, without needing to specify the full bucket.scope.collection syntax.
     */
    public TransactionQueryResult query(Scope scope, String statement) {
        return query(scope, statement, null);
    }
}

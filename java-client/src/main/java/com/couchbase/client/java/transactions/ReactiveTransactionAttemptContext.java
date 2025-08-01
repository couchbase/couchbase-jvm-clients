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

import com.couchbase.client.core.util.ReactorOps;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.query.CoreQueryContext;
import com.couchbase.client.core.api.query.CoreQueryOptions;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.ReactiveScope;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.Transcoder;
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
import reactor.core.publisher.Mono;

import java.util.List;

import static com.couchbase.client.core.cnc.TracingIdentifiers.TRANSACTION_OP_GET_MULTI;
import static com.couchbase.client.core.cnc.TracingIdentifiers.TRANSACTION_OP_GET_MULTI_REPLICAS_FROM_PREFERRED_SERVER_GROUP;
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
import static java.util.Objects.requireNonNull;

/**
 * Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents, as well
 * as commit or rollback the transaction.
 */
public class ReactiveTransactionAttemptContext {
    private static final TransactionGetOptions DEFAULT_GET_OPTIONS = transactionGetOptions();
    private static final TransactionInsertOptions DEFAULT_INSERT_OPTIONS = transactionInsertOptions();
    private static final TransactionReplaceOptions DEFAULT_REPLACE_OPTIONS = transactionReplaceOptions();
    private static final TransactionGetReplicaFromPreferredServerGroupOptions DEFAULT_GET_REPLICA_OPTIONS = transactionGetReplicaFromPreferredServerGroupOptions();
    private static final TransactionGetMultiOptions DEFAULT_GET_MULTI_OPTIONS = transactionGetMultiOptions();
    private static final TransactionGetMultiReplicasFromPreferredServerGroupOptions DEFAULT_GET_MULTI_REPLICA_OPTIONS = transactionGetMultiReplicasFromPreferredServerGroupOptions();

    private final CoreTransactionAttemptContext internal;
    private final JsonSerializer serializer;
    private final ReactorOps reactor;

    ReactiveTransactionAttemptContext(ReactorOps reactor, CoreTransactionAttemptContext internal, JsonSerializer serializer) {
        this.reactor = requireNonNull(reactor);
        this.internal = requireNonNull(internal);
        this.serializer = requireNonNull(serializer);
    }

    @Stability.Internal
    CoreTransactionAttemptContext ctx() {
        return internal;
    }

    /**
     * Gets a document with the specified <code>id</code> and from the specified Couchbase <code>collection</code>.
     * <p>
     * If the document does not exist it will throw a {@link DocumentNotFoundException}.
     *
     * @param collection the Couchbase collection the document exists on
     * @param id         the document's ID
     * @return a <code>TransactionGetResult</code> containing the document
     */
    public Mono<TransactionGetResult> get(ReactiveCollection collection, String id) {
        return get(collection, id, DEFAULT_GET_OPTIONS);
    }

    /**
     * Gets a document with the specified <code>id</code> and from the specified Couchbase <code>collection</code>.
     * <p>
     * If the document does not exist it will throw a {@link DocumentNotFoundException}.
     *
     * @param collection the Couchbase collection the document exists on
     * @param id         the document's ID
     * @param options    options controlling the operation
     * @return a <code>TransactionGetResult</code> containing the document
     */
    public Mono<TransactionGetResult> get(ReactiveCollection collection, String id, TransactionGetOptions options) {
        TransactionGetOptions.Built built = options.build();
        return reactor.publishOnUserScheduler(
            internal.get(makeCollectionIdentifier(collection.async()), id)
                .map(result -> new TransactionGetResult(result, serializer(), built.transcoder()))
        );
    }

    /**
     * A convenience wrapper around {@link #getReplicaFromPreferredServerGroup(ReactiveCollection, String, TransactionGetReplicaFromPreferredServerGroupOptions)}
     * using default options.
     */
    public Mono<TransactionGetResult> getReplicaFromPreferredServerGroup(ReactiveCollection collection, String id) {
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
    public Mono<TransactionGetResult> getReplicaFromPreferredServerGroup(ReactiveCollection collection, String id, TransactionGetReplicaFromPreferredServerGroupOptions options) {
        notNull(options, "Options");
        TransactionGetReplicaFromPreferredServerGroupOptions.Built built = options.build();
        return reactor.publishOnUserScheduler(
            internal.getReplicaFromPreferredServerGroup(makeCollectionIdentifier(collection.async()), id)
                .map(result -> new TransactionGetResult(result, serializer(), built.transcoder()))
        );
    }

    /**
     * A convenience wrapper around {@link #getMulti(List, TransactionGetMultiOptions)} using default options.
     */
    @Stability.Uncommitted
    public Mono<TransactionGetMultiResult> getMulti(List<TransactionGetMultiSpec> specs) {
        return getMulti(specs, DEFAULT_GET_MULTI_OPTIONS);
    }

    @Stability.Uncommitted
    public Mono<TransactionGetMultiResult> getMulti(List<TransactionGetMultiSpec> specs, TransactionGetMultiOptions options) {
        notNull(options, "options");
        return reactor.publishOnUserScheduler(
            internal.getMultiAlgo(TransactionGetMultiUtil.convert(specs), options.build(), false)
                .map(result -> TransactionGetMultiUtil.convert(result, specs, serializer())));
    }

    /**
     * A convenience wrapper around {@link #getMulti(List, TransactionGetMultiOptions)} using default options.
     */
    @Stability.Uncommitted
    public Mono<TransactionGetMultiReplicasFromPreferredServerGroupResult> getMultiReplicasFromPreferredServerGroup(List<TransactionGetMultiReplicasFromPreferredServerGroupSpec> specs) {
        return getMultiReplicasFromPreferredServerGroup(specs, DEFAULT_GET_MULTI_REPLICA_OPTIONS);
    }

    /**
     * Similar to {@link #getMulti(List, TransactionGetMultiOptions)}, but fetches the documents from replicas in the preferred server group.
     * <p>
     * Note that the nature of replicas is that they are eventually consistent with the active, and so the effectiveness of read skew detection may be impacted.
     */
    @Stability.Uncommitted
    public Mono<TransactionGetMultiReplicasFromPreferredServerGroupResult> getMultiReplicasFromPreferredServerGroup(List<TransactionGetMultiReplicasFromPreferredServerGroupSpec> specs, TransactionGetMultiReplicasFromPreferredServerGroupOptions options) {
        notNull(options, "options");
        return reactor.publishOnUserScheduler(
            internal.getMultiAlgo(TransactionGetMultiUtil.convertReplica(specs), options.build(), true)
                .map(result -> TransactionGetMultiUtil.convertReplica(result, specs, serializer())));
    }

    /**
     * Inserts a new document into the specified Couchbase <code>collection</code>.
     *
     * @param collection the Couchbase collection in which to insert the doc
     * @param id         the document's unique ID
     * @param content    the content to insert
     * @return the doc, updated with its new CAS value and ID, and converted to a <code>TransactionGetResult</code>
     */
    public Mono<TransactionGetResult> insert(ReactiveCollection collection, String id, Object content) {
        return insert(collection, id, content, DEFAULT_INSERT_OPTIONS);
    }

    /**
     * Inserts a new document into the specified Couchbase <code>collection</code>.
     *
     * @param collection the Couchbase collection in which to insert the doc
     * @param id         the document's unique ID
     * @param content    the content to insert
     * @param options    options controlling the operation
     * @return the doc, updated with its new CAS value and ID, and converted to a <code>TransactionGetResult</code>
     */
    public Mono<TransactionGetResult> insert(ReactiveCollection collection, String id, Object content, TransactionInsertOptions options) {
        TransactionInsertOptions.Built built = options.build();
        RequestSpan span = CbTracing.newSpan(internal.core().context(), TRANSACTION_OP_INSERT, internal.span());
        span.lowCardinalityAttribute(TracingIdentifiers.ATTR_OPERATION, TRANSACTION_OP_INSERT);
        Transcoder.EncodedValue encoded = encode(content, span, serializer, built.transcoder(), internal.core().context());

        return reactor.publishOnUserScheduler(
            internal.insert(makeCollectionIdentifier(collection.async()), id, encoded.encoded(), encoded.flags(), new SpanWrapper(span))
                .map(result -> new TransactionGetResult(result, serializer(), built.transcoder()))
                .doOnError(err -> span.status(RequestSpan.StatusCode.ERROR))
                .doOnTerminate(() -> span.end())
        );
    }

    private JsonSerializer serializer() {
        return serializer;
    }

    /**
     * Mutates the specified <code>doc</code> with new content.
     *
     * @param doc     the doc to be mutated
     * @param content the content to replace the doc with
     * @return the doc, updated with its new CAS value.  For performance a copy is not created and the original doc
     * object is modified.
     */
    public Mono<TransactionGetResult> replace(TransactionGetResult doc, Object content) {
        return replace(doc, content, DEFAULT_REPLACE_OPTIONS);
    }

    /**
     * Mutates the specified <code>doc</code> with new content.
     *
     * @param doc     the doc to be mutated
     * @param content the content to replace the doc with
     * @param options options controlling the operation
     * @return the doc, updated with its new CAS value.  For performance a copy is not created and the original doc
     * object is modified.
     */
    public Mono<TransactionGetResult> replace(TransactionGetResult doc, Object content, TransactionReplaceOptions options) {
        TransactionReplaceOptions.Built built = options.build();
        RequestSpan span = CbTracing.newSpan(internal.core().context(), TRANSACTION_OP_REPLACE, internal.span());
        span.lowCardinalityAttribute(TracingIdentifiers.ATTR_OPERATION, TRANSACTION_OP_REPLACE);
        Transcoder.EncodedValue encoded = encode(content, span, serializer, built.transcoder(), internal.core().context());
        return reactor.publishOnUserScheduler(
            internal.replace(doc.internal(), encoded.encoded(), encoded.flags(), new SpanWrapper(span))
                .map(result -> new TransactionGetResult(result, serializer(), built.transcoder()))
                .doOnError(err -> span.status(RequestSpan.StatusCode.ERROR))
                .doOnTerminate(() -> span.end())
        );
    }

    /**
     * Removes the specified <code>doc</code>.
     * <p>
     * @param doc - the doc to be removed
     */
    public Mono<Void> remove(TransactionGetResult doc) {
        RequestSpan span = CbTracing.newSpan(internal.core().context(), TRANSACTION_OP_REMOVE, internal.span());
        span.lowCardinalityAttribute(TracingIdentifiers.ATTR_OPERATION, TRANSACTION_OP_REMOVE);
        return reactor.publishOnUserScheduler(
            internal.remove(doc.internal(), new SpanWrapper(span))
                .doOnError(err -> span.status(RequestSpan.StatusCode.ERROR))
                .doOnTerminate(() -> span.end())
        );
    }

    @SuppressWarnings("unused")
    @Stability.Internal
    CoreTransactionLogger logger() {
        return internal.logger();
    }

    /**
     * Calls query() with default options.
     */
    public Mono<TransactionQueryResult> query(final String statement) {
        return query(statement, null);
    }


    /**
     * Runs a N1QL query and returns the result.
     * <p>
     * All rows are buffered in-memory.
     * <p>
     * Raises CouchbaseException or an error derived from it on failure.  The application can choose to catch and ignore this error, and the
     * transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will
     * cause the attempt to fail.
     */
    public Mono<TransactionQueryResult> query(final String statement,
                                              final TransactionQueryOptions options) {
        return query(null, statement, options);
    }

    /**
     * Runs a N1QL query and returns the result, with default parameters.
     * <p>
     * All rows are buffered in-memory.
     * <p>
     * This overload performs a 'scope-level query': that is, one in which a collection may be referenced by name in the
     * query statement, without needing to specify the full bucket.scope.collection syntax.
     * <p>
     * Raises CouchbaseException or an error derived from it on failure.  The application can choose to catch and ignore this error, and the
     * transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will
     * cause the attempt to fail.
     */
    public Mono<TransactionQueryResult> query(final ReactiveScope scope,
                                              final String statement) {
        return query(scope, statement, null);
    }

    /**
     * Runs a N1QL query and returns the result.
     * <p>
     * All rows are buffered in-memory.
     * <p>
     * This overload performs a 'scope-level query': that is, one in which a collection may be referenced by name in the
     * query statement, without needing to specify the full bucket.scope.collection syntax.
     * <p>
     * Raises CouchbaseException or an error derived from it on failure.  The application can choose to catch and ignore this error, and the
     * transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will
     * cause the attempt to fail.
     */
    public Mono<TransactionQueryResult> query(final ReactiveScope scope,
                                              final String statement,
                                              final TransactionQueryOptions options) {
        CoreQueryOptions opts = options != null ? options.builder().build() : null;
        return reactor.publishOnUserScheduler(
            internal.queryBlocking(statement,
                    scope == null ? null : CoreQueryContext.of(scope.bucketName(), scope.name()),
                    opts,
                    false)
                .map(response -> new TransactionQueryResult(response, serializer()))
        );
    }
}

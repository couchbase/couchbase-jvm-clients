/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.manager.query;

import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.core.error.IndexesNotReadyException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.QueryException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.retry.reactor.Retry;
import com.couchbase.client.core.retry.reactor.RetryExhaustedException;
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbThrowables.findCause;
import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static com.couchbase.client.core.util.CbThrowables.throwIfUnchecked;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.manager.query.AsyncQueryIndexManager.QueryType.READ_ONLY;
import static com.couchbase.client.java.manager.query.AsyncQueryIndexManager.QueryType.WRITE;
import static com.couchbase.client.java.manager.query.BuildQueryIndexOptions.buildDeferredQueryIndexesOptions;
import static com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions;
import static com.couchbase.client.java.manager.query.CreateQueryIndexOptions.createQueryIndexOptions;
import static com.couchbase.client.java.manager.query.DropPrimaryQueryIndexOptions.dropPrimaryQueryIndexOptions;
import static com.couchbase.client.java.manager.query.DropQueryIndexOptions.dropQueryIndexOptions;
import static com.couchbase.client.java.manager.query.GetAllQueryIndexesOptions.getAllQueryIndexesOptions;
import static com.couchbase.client.java.manager.query.WatchQueryIndexesOptions.watchQueryIndexesOptions;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

/**
 * Performs management operations on query indexes.
 */
public class AsyncQueryIndexManager {

  /**
   * Holds a reference to the async cluster since query management ops are actually N1QL queries.
   */
  private final AsyncCluster cluster;

  /**
   * Creates a new {@link AsyncQueryIndexManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link AsyncCluster#queryIndexes()}
   * instead.
   *
   * @param cluster the async cluster to perform the queries on.
   */
  @Stability.Internal
  public AsyncQueryIndexManager(final AsyncCluster cluster) {
    this.cluster = requireNonNull(cluster);
  }

  /**
   * Creates a named query index.
   * <p>
   * By default, this method will create an index on the bucket. If an index needs to be created on a collection,
   * both {@link CreateQueryIndexOptions#scopeName(String)} and {@link CreateQueryIndexOptions#collectionName(String)}
   * must be set.
   *
   * @param bucketName the name of the bucket to create the index on.
   * @param indexName the name of the query index.
   * @param fields the collection of fields that are part of the index.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexFailureException (async) if creating the index failed (see reason for details).
   * @throws IndexExistsException (async) if an index already exists with the given name on the keyspace.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createIndex(final String bucketName, final String indexName,
                                             final Collection<String> fields) {
    return createIndex(bucketName, indexName, fields, createQueryIndexOptions());
  }

  /**
   * Creates a named query index with custom options.
   * <p>
   * By default, this method will create an index on the bucket. If an index needs to be created on a collection,
   * both {@link CreateQueryIndexOptions#scopeName(String)} and {@link CreateQueryIndexOptions#collectionName(String)}
   * must be set.
   *
   * @param bucketName the name of the bucket to create the index on.
   * @param indexName the name of the query index.
   * @param fields the collection of fields that are part of the index.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexFailureException (async) if creating the index failed (see reason for details).
   * @throws IndexExistsException (async) if an index already exists with the given name on the keyspace.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createIndex(final String bucketName, final String indexName,
                                             final Collection<String> fields, final CreateQueryIndexOptions options) {
    notNullOrEmpty(bucketName, "BucketName");
    notNullOrEmpty(indexName, "IndexName");
    notNullOrEmpty(fields, "Fields");
    notNull(options, "Options");

    final CreateQueryIndexOptions.Built builtOpts = options.build();
    final String keyspace = buildKeyspace(bucketName, builtOpts.scopeName(), builtOpts.collectionName());
    final String statement = "CREATE INDEX " + quote(indexName) + " ON " + keyspace + formatIndexFields(fields);

    return exec(WRITE, statement, builtOpts.with(), builtOpts, TracingIdentifiers.SPAN_REQUEST_MQ_CREATE_INDEX, bucketName, null)
        .exceptionally(t -> {
          if (builtOpts.ignoreIfExists() && hasCause(t, IndexExistsException.class)) {
            return null;
          }
          throwIfUnchecked(t);
          throw new RuntimeException(t);
        })
        .thenApply(result -> null);
  }

  /**
   * Creates a primary query index.
   * <p>
   * By default, this method will create an index on the bucket. If an index needs to be created on a collection,
   * both {@link CreatePrimaryQueryIndexOptions#scopeName(String)} and
   * {@link CreatePrimaryQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to create the index on.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexFailureException (async) if creating the index failed (see reason for details).
   * @throws IndexExistsException (async) if an index already exists with the given name on the keyspace.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createPrimaryIndex(final String bucketName) {
    return createPrimaryIndex(bucketName, createPrimaryQueryIndexOptions());
  }

  /**
   * Creates a primary query index with custom options.
   * <p>
   * By default, this method will create an index on the bucket. If an index needs to be created on a collection,
   * both {@link CreatePrimaryQueryIndexOptions#scopeName(String)} and
   * {@link CreatePrimaryQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to create the index on.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexFailureException (async) if creating the index failed (see reason for details).
   * @throws IndexExistsException (async) if an index already exists with the given name on the keyspace.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createPrimaryIndex(final String bucketName,
                                                    final CreatePrimaryQueryIndexOptions options) {
    notNullOrEmpty(bucketName, "BucketName");
    notNull(options, "Options");

    final CreatePrimaryQueryIndexOptions.Built builtOpts = options.build();
    final String indexName = builtOpts.indexName().orElse(null);
    final String keyspace = buildKeyspace(bucketName, builtOpts.scopeName(), builtOpts.collectionName());

    String statement = "CREATE PRIMARY INDEX ";
    if (indexName != null) {
      statement += quote(indexName) + " ";
    }
    statement += "ON " + keyspace;

    return exec(WRITE, statement, builtOpts.with(), builtOpts, TracingIdentifiers.SPAN_REQUEST_MQ_CREATE_PRIMARY_INDEX, bucketName, null)
        .exceptionally(t -> {
          if (builtOpts.ignoreIfExists() && hasCause(t, IndexExistsException.class)) {
            return null;
          }
          throwIfUnchecked(t);
          throw new RuntimeException(t);
        })
        .thenApply(result -> null);
  }

  /**
   * Fetches all indexes from the bucket.
   * <p>
   * By default, this method will fetch all index on the bucket. If the indexes should be loaded for a collection,
   * both {@link GetAllQueryIndexesOptions#scopeName(String)} and
   * {@link GetAllQueryIndexesOptions#collectionName(String)} must be set. If all indexes for a scope should be loaded,
   * only the {@link GetAllQueryIndexesOptions#scopeName(String)} can be set.
   *
   * @param bucketName the name of the bucket to load the indexes from.
   * @return a {@link CompletableFuture} completing with a list of (potentially empty) indexes or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<QueryIndex>> getAllIndexes(final String bucketName) {
    return getAllIndexes(bucketName, getAllQueryIndexesOptions());
  }

  /**
   * Fetches all indexes from the bucket with custom options.
   * <p>
   * By default, this method will fetch all index on the bucket. If the indexes should be loaded for a collection,
   * both {@link GetAllQueryIndexesOptions#scopeName(String)} and
   * {@link GetAllQueryIndexesOptions#collectionName(String)} must be set. If all indexes for a scope should be loaded,
   * only the {@link GetAllQueryIndexesOptions#scopeName(String)} can be set.
   *
   * @param bucketName the name of the bucket to load the indexes from.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing with a list of (potentially empty) indexes or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<QueryIndex>> getAllIndexes(final String bucketName,
                                                           final GetAllQueryIndexesOptions options) {
    notNullOrEmpty(bucketName, "BucketName");
    notNull(options, "Options");
    final GetAllQueryIndexesOptions.Built builtOpts = options.build();

    String statement;
    JsonArray params;
    if (builtOpts.scopeName().isPresent() && builtOpts.collectionName().isPresent()) {
      statement = "SELECT idx.* FROM system:indexes AS idx" +
        " WHERE keyspace_id = ? AND bucket_id = ? AND scope_id = ?" +
        " AND `using` = \"gsi\"" +
        " ORDER BY is_primary DESC, name ASC";
      params = JsonArray.from(builtOpts.collectionName().get(), bucketName, builtOpts.scopeName().get());
    } else if (builtOpts.scopeName().isPresent()) {
      statement = "SELECT idx.* FROM system:indexes AS idx" +
        " WHERE bucket_id = ? AND scope_id = ?" +
        " AND `using` = \"gsi\"" +
        " ORDER BY is_primary DESC, name ASC";
      params = JsonArray.from(bucketName, builtOpts.scopeName().get());
    } else {
      statement = "SELECT idx.* FROM system:indexes AS idx" +
        " WHERE ((bucket_id IS MISSING AND keyspace_id = ?) OR bucket_id = ?)" +
        " AND `using` = \"gsi\"" +
        " ORDER BY is_primary DESC, name ASC";
      params = JsonArray.from(bucketName, bucketName);
    }

    return exec(READ_ONLY, statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MQ_GET_ALL_INDEXES, bucketName, params)
        .thenApply(result -> result.rowsAsObject().stream()
            .map(QueryIndex::new)
            .collect(toList()));
  }

  /**
   * Drops the primary index from a bucket.
   * <p>
   * By default, this method will drop the primary index on the bucket. If the index should be dropped on a collection,
   * both {@link DropPrimaryQueryIndexOptions#scopeName(String)} and
   * {@link DropPrimaryQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to drop the indexes from.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist.
   * @throws IndexFailureException (async) if dropping the index failed (see reason for details).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropPrimaryIndex(final String bucketName) {
    return dropPrimaryIndex(bucketName, dropPrimaryQueryIndexOptions());
  }

  /**
   * Drops the primary index from a bucket with custom options.
   * <p>
   * By default, this method will drop the primary index on the bucket. If the index should be dropped on a collection,
   * both {@link DropPrimaryQueryIndexOptions#scopeName(String)} and
   * {@link DropPrimaryQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to drop the indexes from.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist.
   * @throws IndexFailureException (async) if dropping the index failed (see reason for details).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropPrimaryIndex(final String bucketName, final DropPrimaryQueryIndexOptions options) {
    notNullOrEmpty(bucketName, "BucketName");
    notNull(options, "Options");

    final DropPrimaryQueryIndexOptions.Built builtOpts = options.build();
    final String keyspace = buildKeyspace(bucketName, builtOpts.scopeName(), builtOpts.collectionName());
    final String statement = "DROP PRIMARY INDEX ON " + keyspace;

    return exec(WRITE, statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MQ_DROP_PRIMARY_INDEX, bucketName, null)
        .exceptionally(t -> {
          if (builtOpts.ignoreIfNotExists() && hasCause(t, IndexNotFoundException.class)) {
            return null;
          }
          throwIfUnchecked(t);
          throw new RuntimeException(t);
        })
        .thenApply(result -> null);
  }

  /**
   * Drops a query index from a bucket.
   * <p>
   * By default, this method will drop the index on the bucket. If the index should be dropped on a collection,
   * both {@link DropQueryIndexOptions#scopeName(String)} and
   * {@link DropQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to drop the indexes from.
   * @param indexName the name of the index top drop.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist.
   * @throws IndexFailureException (async) if dropping the index failed (see reason for details).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropIndex(final String bucketName, final String indexName) {
    return dropIndex(bucketName, indexName, dropQueryIndexOptions());
  }

  /**
   * Drops a query index from a bucket with custom options.
   * <p>
   * By default, this method will drop the index on the bucket. If the index should be dropped on a collection,
   * both {@link DropQueryIndexOptions#scopeName(String)} and
   * {@link DropQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to drop the indexes from.
   * @param indexName the name of the index top drop.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist.
   * @throws IndexFailureException (async) if dropping the index failed (see reason for details).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropIndex(final String bucketName, final String indexName,
                                           final DropQueryIndexOptions options) {
    notNullOrEmpty(bucketName, "BucketName");
    notNullOrEmpty(indexName, "IndexName");
    notNull(options, "Options");

    final DropQueryIndexOptions.Built builtOpts = options.build();
    final String statement = builtOpts.scopeName().isPresent() && builtOpts.collectionName().isPresent()
      ? "DROP INDEX " + quote(indexName) + " ON " + buildKeyspace(bucketName, builtOpts.scopeName(), builtOpts.collectionName())
      : "DROP INDEX " + quote(bucketName, indexName);

    return exec(WRITE, statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MQ_DROP_INDEX, bucketName, null)
        .exceptionally(t -> {
          if (builtOpts.ignoreIfNotExists() && hasCause(t, IndexNotFoundException.class)) {
            return null;
          }
          throwIfUnchecked(t);
          throw new RuntimeException(t);
        })
        .thenApply(result -> null);
  }

  /**
   * Builds all currently deferred indexes.
   * <p>
   * By default, this method will build the indexes on the bucket. If the indexes should be built on a collection,
   * both {@link BuildQueryIndexOptions#scopeName(String)} and
   * {@link BuildQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to build deferred indexes for.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> buildDeferredIndexes(final String bucketName) {
    return buildDeferredIndexes(bucketName, buildDeferredQueryIndexesOptions());
  }

  /**
   * Builds all currently deferred indexes.
   * <p>
   * By default, this method will build the indexes on the bucket. If the indexes should be built on a collection,
   * both {@link BuildQueryIndexOptions#scopeName(String)} and
   * {@link BuildQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to build deferred indexes for.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> buildDeferredIndexes(final String bucketName, final BuildQueryIndexOptions options) {
    notNullOrEmpty(bucketName, "BucketName");
    notNull(options, "Options");
    final BuildQueryIndexOptions.Built builtOpts = options.build();


    GetAllQueryIndexesOptions getAllOptions = getAllQueryIndexesOptions();
    builtOpts.collectionName().ifPresent(getAllOptions::collectionName);
    builtOpts.scopeName().ifPresent(getAllOptions::scopeName);
    builtOpts.timeout().ifPresent(getAllOptions::timeout);

    return Reactor
      .toMono(() -> getAllIndexes(bucketName, getAllOptions))
      .map(indexes -> indexes
        .stream()
        .filter(idx -> idx.state().equals("deferred"))
        .map(idx -> quote(idx.name()))
        .collect(Collectors.toList())
      )
      .flatMap(indexNames -> {
        if (indexNames.isEmpty()) {
          return Mono.empty();
        }

        String keyspace = builtOpts.collectionName().isPresent() && builtOpts.scopeName().isPresent()
          ? buildKeyspace(bucketName, builtOpts.scopeName(), builtOpts.collectionName())
          : quote(bucketName);

        String statement = "BUILD INDEX ON " + keyspace + " (" + String.join(",", indexNames) + ")";

        return Reactor.toMono(
          () -> exec(WRITE, statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MQ_BUILD_DEFERRED_INDEXES, bucketName, null)
          .thenApply(result -> null)
        );
      })
      .then()
      .toFuture();
  }

  /**
   * Watches/Polls indexes until they are online.
   * <p>
   * By default, this method will watch the indexes on the bucket. If the indexes should be watched on a collection,
   * both {@link WatchQueryIndexesOptions#scopeName(String)} and
   * {@link WatchQueryIndexesOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket where the indexes should be watched.
   * @param indexNames the names of the indexes to watch.
   * @param timeout the maximum amount of time the indexes should be watched.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> watchIndexes(final String bucketName, final Collection<String> indexNames,
                                              final Duration timeout) {
    return watchIndexes(bucketName, indexNames, timeout, watchQueryIndexesOptions());
  }

  /**
   * Watches/Polls indexes until they are online with custom options.
   * <p>
   * By default, this method will watch the indexes on the bucket. If the indexes should be watched on a collection,
   * both {@link WatchQueryIndexesOptions#scopeName(String)} and
   * {@link WatchQueryIndexesOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket where the indexes should be watched.
   * @param indexNames the names of the indexes to watch.
   * @param timeout the maximum amount of time the indexes should be watched.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> watchIndexes(final String bucketName, final Collection<String> indexNames,
                                              final Duration timeout, final WatchQueryIndexesOptions options) {
    notNullOrEmpty(bucketName, "BucketName");
    notNull(indexNames, "IndexNames");
    notNull(timeout, "Timeout");
    notNull(options, "Options");

    Set<String> indexNameSet = new HashSet<>(indexNames);
    WatchQueryIndexesOptions.Built builtOpts = options.build();

    RequestSpan parent = cluster.environment().requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_MQ_WATCH_INDEXES, null);
    parent.attribute(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);

    return Mono.fromFuture(() -> failIfIndexesOffline(bucketName, indexNameSet, builtOpts.watchPrimary(), parent,
        builtOpts.scopeName(), builtOpts.collectionName()))
        .retryWhen(Retry.onlyIf(ctx -> hasCause(ctx.exception(), IndexesNotReadyException.class))
            .exponentialBackoff(Duration.ofMillis(50), Duration.ofSeconds(1))
            .timeout(timeout)
            .toReactorRetry())
        .onErrorMap(t -> t instanceof RetryExhaustedException ? toWatchTimeoutException(t, timeout) : t)
        .toFuture()
        .whenComplete((r, t) -> parent.end());
  }

  private static String formatIndexFields(Collection<String> fields) {
    return "(" + String.join(",", fields) + ")";
  }

  private static TimeoutException toWatchTimeoutException(Throwable t, Duration timeout) {
    final StringBuilder msg = new StringBuilder("A requested index is still not ready after " + timeout + ".");

    findCause(t, IndexesNotReadyException.class).ifPresent(cause ->
        msg.append(" Unready index name -> state: ").append(redactMeta(cause.indexNameToState())));

    return new TimeoutException(msg.toString());
  }

  private CompletableFuture<Void> failIfIndexesOffline(final String bucketName, final Set<String> indexNames,
                                                       final boolean includePrimary, final RequestSpan parentSpan,
                                                       final Optional<String> scopeName, final Optional<String> collectionName)
    throws IndexesNotReadyException, IndexNotFoundException {

    requireNonNull(bucketName);
    requireNonNull(indexNames);

    GetAllQueryIndexesOptions getAllQueryIndexesOptions = getAllQueryIndexesOptions().parentSpan(parentSpan);
    scopeName.ifPresent(getAllQueryIndexesOptions::scopeName);
    collectionName.ifPresent(getAllQueryIndexesOptions::collectionName);

    return getAllIndexes(bucketName, getAllQueryIndexesOptions)
        .thenApply(allIndexes -> {
          final List<QueryIndex> matchingIndexes = allIndexes.stream()
              .filter(idx -> indexNames.contains(idx.name()) || (includePrimary && idx.primary()))
              .collect(toList());

          final boolean primaryIndexPresent = matchingIndexes.stream()
              .anyMatch(QueryIndex::primary);

          if (includePrimary && !primaryIndexPresent) {
            throw new IndexNotFoundException("#primary");
          }

          final Set<String> matchingIndexNames = matchingIndexes.stream()
              .map(QueryIndex::name)
              .collect(toSet());

          final Set<String> missingIndexNames = difference(indexNames, matchingIndexNames);
          if (!missingIndexNames.isEmpty()) {
            throw new IndexNotFoundException(missingIndexNames.toString());
          }

          final Map<String, String> offlineIndexNameToState = matchingIndexes.stream()
              .filter(idx -> !"online".equals(idx.state()))
              .collect(toMap(QueryIndex::name, QueryIndex::state));

          if (!offlineIndexNameToState.isEmpty()) {
            throw new IndexesNotReadyException(offlineIndexNameToState);
          }

          return null;
        });
  }

  /**
   * Returns a set containing all items in {@code lhs} that are not also in {@code rhs}.
   */
  private static <T> Set<T> difference(Set<T> lhs, Set<T> rhs) {
    Set<T> result = new HashSet<>(lhs);
    result.removeAll(rhs);
    return result;
  }

  private CompletableFuture<QueryResult> exec(QueryType queryType, CharSequence statement, Map<String, Object> with,
                                              CommonOptions<?>.BuiltCommonOptions options, String spanName, String bucketName,
                                              JsonArray parameters) {
    return with.isEmpty()
        ? exec(queryType, statement, options, spanName, bucketName, parameters)
        : exec(queryType, statement + " WITH " + Mapper.encodeAsString(with), options, spanName, bucketName, parameters);
  }

  private CompletableFuture<QueryResult> exec(QueryType queryType, CharSequence statement,
                                              CommonOptions<?>.BuiltCommonOptions options, String spanName, String bucketName,
                                              JsonArray parameters) {
    QueryOptions queryOpts = toQueryOptions(options)
        .readonly(requireNonNull(queryType) == READ_ONLY);

    if (parameters != null && !parameters.isEmpty()) {
      queryOpts.parameters(parameters);
    }

    RequestSpan parent = cluster.environment().requestTracer().requestSpan(spanName, options.parentSpan().orElse(null));
    parent.attribute(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);

    if (bucketName != null) {
      parent.attribute(TracingIdentifiers.ATTR_NAME, bucketName);
    }
    queryOpts.parentSpan(parent);

    return cluster
      .query(statement.toString(), queryOpts)
      .exceptionally(t -> {
        throw translateException(t);
      })
      .whenComplete((r, t) -> parent.end());
  }

  private static QueryOptions toQueryOptions(CommonOptions<?>.BuiltCommonOptions options) {
    QueryOptions result = QueryOptions.queryOptions();
    options.timeout().ifPresent(result::timeout);
    options.retryStrategy().ifPresent(result::retryStrategy);
    result.clientContext(options.clientContext());
    return result;
  }

  private static final Map<Predicate<QueryException>, Function<QueryException, ? extends QueryException>> errorMessageMap = new LinkedHashMap<>();

  private RuntimeException translateException(Throwable t) {
    if (t instanceof QueryException) {
      final QueryException e = ((QueryException) t);

      for (Map.Entry<Predicate<QueryException>, Function<QueryException, ? extends QueryException>> entry : errorMessageMap.entrySet()) {
        if (entry.getKey().test(e)) {
          return entry.getValue().apply(e);
        }
      }
    }
    return (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t);
  }

  private static String quote(String s) {
    if (s.contains("`")) {
      throw InvalidArgumentException.fromMessage("Value [" + redactMeta(s) + "] may not contain backticks.");
    }
    return "`" + s + "`";
  }

  private static String quote(String... components) {
    return Arrays.stream(components)
        .map(AsyncQueryIndexManager::quote)
        .collect(Collectors.joining("."));
  }

  private static String buildKeyspace(final String bucket, final Optional<String> scope,
                                      final Optional<String> collection) {
    if (scope.isPresent() && collection.isPresent()) {
      return quote(bucket, scope.get(), collection.get());
    } else {
      return quote(bucket);
    }
  }

  enum QueryType {
    READ_ONLY,
    WRITE
  }

}

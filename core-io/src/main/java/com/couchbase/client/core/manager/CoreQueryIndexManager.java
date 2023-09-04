/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.manager;

import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.manager.CoreBuildQueryIndexOptions;
import com.couchbase.client.core.api.manager.CoreCreatePrimaryQueryIndexOptions;
import com.couchbase.client.core.api.manager.CoreCreateQueryIndexOptions;
import com.couchbase.client.core.api.manager.CoreDropPrimaryQueryIndexOptions;
import com.couchbase.client.core.api.manager.CoreDropQueryIndexOptions;
import com.couchbase.client.core.api.manager.CoreGetAllQueryIndexesOptions;
import com.couchbase.client.core.api.manager.CoreQueryIndex;
import com.couchbase.client.core.api.manager.CoreScopeAndCollection;
import com.couchbase.client.core.api.manager.CoreWatchQueryIndexesOptions;
import com.couchbase.client.core.api.query.CoreQueryOps;
import com.couchbase.client.core.api.query.CoreQueryOptions;
import com.couchbase.client.core.api.query.CoreQueryResult;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.IndexesNotReadyException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.retry.reactor.Retry;
import com.couchbase.client.core.retry.reactor.RetryExhaustedException;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION;
import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE;
import static com.couchbase.client.core.manager.CoreCollectionQueryIndexManager.createIndexWith;
import static com.couchbase.client.core.manager.CoreCollectionQueryIndexManager.failIfIndexesOfflineHelper;
import static com.couchbase.client.core.manager.CoreCollectionQueryIndexManager.formatIndexFields;
import static com.couchbase.client.core.manager.CoreCollectionQueryIndexManager.quote;
import static com.couchbase.client.core.manager.CoreCollectionQueryIndexManager.toWatchTimeoutException;
import static com.couchbase.client.core.manager.CoreQueryType.READ_ONLY;
import static com.couchbase.client.core.manager.CoreQueryType.WRITE;
import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static com.couchbase.client.core.util.CbThrowables.throwIfUnchecked;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Stability.Internal
public class CoreQueryIndexManager {
  private final RequestTracer requestTracer;
  private final CoreQueryOps queryOps;

  public CoreQueryIndexManager(CoreQueryOps queryOps, RequestTracer requestTracer) {
    this.requestTracer = requireNonNull(requestTracer);
    this.queryOps = requireNonNull(queryOps);
  }

  public static ObjectNode getParamsForGetAllIndexes(
          String bucket,
          @Nullable String scope,
          @Nullable String collection
  ) {
    ObjectNode params = Mapper.createObjectNode();
    params.put("bucketName", bucket);
    params.put("scopeName", scope);
    params.put("collectionName", collection);
    return params;
  }


  /**
   * Only here temporarily for Kotlin and Scala, will be removed.
   */
  @Deprecated
  public static Map<String, String> getNamedParamsForGetAllIndexes(
          @Nullable String bucket,
          @Nullable String scope,
          @Nullable String collection
  ) {
    Map<String, String> params = new HashMap<>();
    params.put("bucketName", bucket);
    params.put("scopeName", scope);
    params.put("collectionName", collection);
    return params;
  }

  public static String getStatementForGetAllIndexes(
          String bucket,
          @Nullable String scope,
          @Nullable String collection) {

    if (collection != null && scope == null) {
      throw new InvalidArgumentException("When collection is non-null, scope must also be non-null.", null, null);
    }

    String bucketCondition = "(bucket_id = $bucketName)";
    String scopeCondition = "(" + bucketCondition + " AND scope_id = $scopeName)";
    String collectionCondition = "(" + scopeCondition + " AND keyspace_id = $collectionName)";

    String whereCondition;
    if (collection != null) {
      whereCondition = collectionCondition;
    } else if (scope != null) {
      whereCondition = scopeCondition;
    } else {
      whereCondition = bucketCondition;
    }

    // If indexes on the default collection should be included in the results,
    // modify the query to match the irregular structure of those indexes.
    if (DEFAULT_COLLECTION.equals(collection) || collection == null) {
      String defaultCollectionCondition = "(bucket_id IS MISSING AND keyspace_id = $bucketName)";
      whereCondition = "(" + whereCondition + " OR " + defaultCollectionCondition + ")";
    }

    return "SELECT idx.* FROM system:indexes AS idx" +
            " WHERE " + whereCondition +
            " AND `using` = \"gsi\"" +
            " ORDER BY is_primary DESC, name ASC";
  }

  public CompletableFuture<Void> createIndex(final String bucketName, final String indexName,
                                             final Collection<String> fields, final CoreCreateQueryIndexOptions options) {
    notNullOrEmpty(bucketName, "BucketName");
    notNullOrEmpty(indexName, "IndexName");
    notNullOrEmpty(fields, "Fields");
    notNull(options, "Options");

    final String keyspace = buildKeyspace(bucketName, options.scopeAndCollection());
    final String statement = "CREATE INDEX " + quote(indexName) + " ON " + keyspace + formatIndexFields(fields);
    final Map<String, Object> with = createIndexWith(options);

    return exec(WRITE, statement, with, options.commonOptions(), TracingIdentifiers.SPAN_REQUEST_MQ_CREATE_INDEX, bucketName, null)
            .exceptionally(t -> {
              if (options.ignoreIfExists() && hasCause(t, IndexExistsException.class)) {
                return null;
              }
              throwIfUnchecked(t);
              throw new RuntimeException(t);
            })
            .thenApply(result -> null);
  }

  public CompletableFuture<Void> createPrimaryIndex(final String bucketName,
                                                    final CoreCreatePrimaryQueryIndexOptions options) {
    notNullOrEmpty(bucketName, "BucketName");
    notNull(options, "Options");

    final String keyspace = buildKeyspace(bucketName, options.scopeAndCollection());

    String statement = "CREATE PRIMARY INDEX ";
    if (options.indexName() != null) {
      statement += quote(options.indexName()) + " ";
    }
    statement += "ON " + keyspace;
    Map<String, Object> with = createIndexWith(options);

    return exec(WRITE, statement, with, options.commonOptions(), TracingIdentifiers.SPAN_REQUEST_MQ_CREATE_PRIMARY_INDEX, bucketName, null)
            .exceptionally(t -> {
              if (options.ignoreIfExists() && hasCause(t, IndexExistsException.class)) {
                return null;
              }
              throwIfUnchecked(t);
              throw new RuntimeException(t);
            })
            .thenApply(result -> null);
  }

  public CompletableFuture<List<CoreQueryIndex>> getAllIndexes(final String bucketName,
                                                               final CoreGetAllQueryIndexesOptions options) {
    notNullOrEmpty(bucketName, "BucketName");
    notNull(options, "Options");

    String statement = getStatementForGetAllIndexes(bucketName, options.scopeName(), options.collectionName());
    ObjectNode params = getParamsForGetAllIndexes(bucketName, options.scopeName(), options.collectionName());

    return exec(READ_ONLY, statement, options.commonOptions(), TracingIdentifiers.SPAN_REQUEST_MQ_GET_ALL_INDEXES, bucketName, params)
            .thenApply(result -> result.rows()
                    .map(CoreQueryIndex::new)
                    .collect(toList()));
  }

  public CompletableFuture<Void> dropPrimaryIndex(final String bucketName, final CoreDropPrimaryQueryIndexOptions options) {
    notNullOrEmpty(bucketName, "BucketName");
    notNull(options, "Options");

    final String keyspace = buildKeyspace(bucketName, options.scopeAndCollection());
    final String statement = "DROP PRIMARY INDEX ON " + keyspace;

    return exec(WRITE, statement, options.commonOptions(), TracingIdentifiers.SPAN_REQUEST_MQ_DROP_PRIMARY_INDEX, bucketName, null)
            .exceptionally(t -> {
              if (options.ignoreIfNotExists() && hasCause(t, IndexNotFoundException.class)) {
                return null;
              }
              throwIfUnchecked(t);
              throw new RuntimeException(t);
            })
            .thenApply(result -> null);
  }

  public CompletableFuture<Void> dropIndex(final String bucketName, final String indexName,
                                           final CoreDropQueryIndexOptions options) {
    notNullOrEmpty(bucketName, "BucketName");
    notNullOrEmpty(indexName, "IndexName");
    notNull(options, "Options");

    final String statement = options.scopeAndCollection() != null
            ? "DROP INDEX " + quote(indexName) + " ON " + buildKeyspace(bucketName, options.scopeAndCollection())
            : "DROP INDEX " + quote(bucketName, indexName);

    return exec(WRITE, statement, options.commonOptions(), TracingIdentifiers.SPAN_REQUEST_MQ_DROP_INDEX, bucketName, null)
            .exceptionally(t -> {
              if (options.ignoreIfNotExists() && hasCause(t, IndexNotFoundException.class)) {
                return null;
              }
              throwIfUnchecked(t);
              throw new RuntimeException(t);
            })
            .thenApply(result -> null);
  }

  public CompletableFuture<Void> buildDeferredIndexes(final String bucketName, final CoreBuildQueryIndexOptions options) {
    notNullOrEmpty(bucketName, "BucketName");
    notNull(options, "Options");

    // Always specify a non-null scope and collection when building the options for getAllQueryIndexes,
    // otherwise it returns indexes from all collections in the bucket.
    CoreGetAllQueryIndexesOptions getAllOptions = new CoreGetAllQueryIndexesOptions() {
      @Override
      public String scopeName() {
        return options.scopeAndCollection() != null ? options.scopeAndCollection().scopeName() : DEFAULT_SCOPE;
      }

      @Override
      public String collectionName() {
        return options.scopeAndCollection() != null ? options.scopeAndCollection().collectionName() : DEFAULT_COLLECTION;
      }

      @Override
      public CoreCommonOptions commonOptions() {
        return options.commonOptions();
      }
    };

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

              String keyspace = options.scopeAndCollection() != null
                      ? buildKeyspace(bucketName, options.scopeAndCollection())
                      : quote(bucketName);

              String statement = "BUILD INDEX ON " + keyspace + " (" + String.join(",", indexNames) + ")";

              return Reactor.toMono(
                      () -> exec(WRITE, statement, options.commonOptions(), TracingIdentifiers.SPAN_REQUEST_MQ_BUILD_DEFERRED_INDEXES, bucketName, null)
                              .thenApply(result -> null)
              );
            })
            .then()
            .toFuture();
  }

  public CompletableFuture<Void> watchIndexes(final String bucketName, final Collection<String> indexNames,
                                              final Duration timeout, final CoreWatchQueryIndexesOptions options) {
    notNullOrEmpty(bucketName, "BucketName");
    notNull(indexNames, "IndexNames");
    notNull(timeout, "Timeout");
    notNull(options, "Options");

    Set<String> indexNameSet = new HashSet<>(indexNames);

    RequestSpan parent = requestTracer.requestSpan(TracingIdentifiers.SPAN_REQUEST_MQ_WATCH_INDEXES, null);
    parent.lowCardinalityAttribute(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);

    return Mono.fromFuture(() -> failIfIndexesOffline(bucketName, indexNameSet, options.watchPrimary(), parent, options.scopeAndCollection()))
            .retryWhen(Retry.onlyIf(ctx -> hasCause(ctx.exception(), IndexesNotReadyException.class))
                    .exponentialBackoff(Duration.ofMillis(50), Duration.ofSeconds(1))
                    .timeout(timeout)
                    .toReactorRetry())
            .onErrorMap(t -> toWatchTimeoutException(t, timeout))
            .toFuture()
            .whenComplete((r, t) -> parent.end());
  }

  private CompletableFuture<Void> failIfIndexesOffline(final String bucketName, final Set<String> indexNames,
                                                       final boolean includePrimary, final RequestSpan parentSpan,
                                                       final CoreScopeAndCollection scopeAndCollection)
          throws IndexesNotReadyException, IndexNotFoundException {

    requireNonNull(bucketName);
    requireNonNull(indexNames);

    CoreGetAllQueryIndexesOptions getAllQueryIndexesOptions = new CoreGetAllQueryIndexesOptions() {
      @Override
      public String scopeName() {
        return scopeAndCollection != null ? scopeAndCollection.scopeName() : DEFAULT_SCOPE;
      }

      @Override
      public String collectionName() {
        return scopeAndCollection != null ? scopeAndCollection.collectionName() : DEFAULT_COLLECTION;
      }

      @Override
      public CoreCommonOptions commonOptions() {
        return CoreCommonOptions.of(null, null, parentSpan);
      }
    };

    return getAllIndexes(bucketName, getAllQueryIndexesOptions)
            .thenApply(allIndexes -> failIfIndexesOfflineHelper(indexNames, includePrimary, allIndexes));
  }

  private CompletableFuture<CoreQueryResult> exec(CoreQueryType queryType, CharSequence statement, @Nullable Map<String, Object> with,
                                                  CoreCommonOptions options, String spanName, String bucketName,
                                                  ObjectNode parameters) {
    return (with == null || with.isEmpty())
            ? exec(queryType, statement, options, spanName, bucketName, parameters)
            : exec(queryType, statement + " WITH " + Mapper.encodeAsString(with), options, spanName, bucketName, parameters);
  }

  private CompletableFuture<CoreQueryResult> exec(CoreQueryType queryType, CharSequence statement,
                                                  CoreCommonOptions options, String spanName, String bucketName,
                                                  ObjectNode parameters) {
    RequestSpan parent = requestTracer.requestSpan(spanName, options.parentSpan().orElse(null));
    parent.lowCardinalityAttribute(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);

    CoreCommonOptions common = CoreCommonOptions.ofOptional(options.timeout(), options.retryStrategy(), Optional.of(parent));

    CoreQueryOptions queryOpts = toQueryOptions(common, requireNonNull(queryType) == READ_ONLY, parameters);

    if (bucketName != null) {
      parent.attribute(TracingIdentifiers.ATTR_NAME, bucketName);
    }

    return queryOps
            .queryAsync(statement.toString(), queryOpts, null, null, null)
            .toFuture()
            .whenComplete((r, t) -> parent.end());
  }

  private static CoreQueryOptions toQueryOptions(CoreCommonOptions options, boolean readonly, ObjectNode parameters) {
    return CoreCollectionQueryIndexManager.toQueryOptions(options, readonly, parameters);
  }

  private static String buildKeyspace(final String bucket, final @Nullable CoreScopeAndCollection scopeAndCollection) {
    if (scopeAndCollection != null) {
      return quote(bucket, scopeAndCollection.scopeName(), scopeAndCollection.collectionName());
    } else {
      return quote(bucket);
    }
  }
}

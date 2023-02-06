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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.manager.CoreBuildQueryIndexOptions;
import com.couchbase.client.core.api.manager.CoreCreatePrimaryQueryIndexOptions;
import com.couchbase.client.core.api.manager.CoreCreateQueryIndexOptions;
import com.couchbase.client.core.api.manager.CoreCreateQueryIndexSharedOptions;
import com.couchbase.client.core.api.manager.CoreDropPrimaryQueryIndexOptions;
import com.couchbase.client.core.api.manager.CoreDropQueryIndexOptions;
import com.couchbase.client.core.api.manager.CoreGetAllQueryIndexesOptions;
import com.couchbase.client.core.api.manager.CoreQueryIndex;
import com.couchbase.client.core.api.manager.CoreScopeAndCollection;
import com.couchbase.client.core.api.manager.CoreWatchQueryIndexesOptions;
import com.couchbase.client.core.api.query.CoreQueryContext;
import com.couchbase.client.core.api.query.CoreQueryOps;
import com.couchbase.client.core.api.query.CoreQueryOptions;
import com.couchbase.client.core.api.query.CoreQueryProfile;
import com.couchbase.client.core.api.query.CoreQueryResult;
import com.couchbase.client.core.api.query.CoreQueryScanConsistency;
import com.couchbase.client.core.api.shared.CoreMutationState;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.IndexesNotReadyException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.retry.reactor.Retry;
import com.couchbase.client.core.retry.reactor.RetryExhaustedException;
import com.couchbase.client.core.transaction.config.CoreSingleQueryTransactionOptions;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.manager.CoreQueryType.READ_ONLY;
import static com.couchbase.client.core.manager.CoreQueryType.WRITE;
import static com.couchbase.client.core.util.CbThrowables.findCause;
import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static com.couchbase.client.core.util.CbThrowables.throwIfUnchecked;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

@Stability.Internal
public class CoreCollectionQueryIndexManager {
  private final Core core;
  private final CoreQueryOps queryOps;

  private final CoreKeyspace collection;
  private final CoreQueryContext queryContext;

  public CoreCollectionQueryIndexManager(Core core, CoreKeyspace collection) {
    this.core = core;
    this.queryOps = core.queryOps();
    this.collection = collection;
    this.queryContext = CoreQueryContext.of(collection.bucket(), collection.scope());
  }

  public ObjectNode getNamedParamsForGetAllIndexes() {
    ObjectNode params = Mapper.createObjectNode();
    params.put("bucketName", collection.bucket());
    params.put("scopeName", collection.scope());
    params.put("collectionName", collection.collection());
    return params;
  }

  public String getStatementForGetAllIndexes() {
    String whereCondition = "(bucket_id = $bucketName AND scope_id = $scopeName AND keyspace_id = $collectionName)";

    // If indexes on the default collection should be included in the results,
    // modify the query to match the irregular structure of those indexes.
    if (collection.isDefaultCollection()) {
      String defaultCollectionCondition = "(bucket_id IS MISSING AND keyspace_id = $bucketName)";
      whereCondition = "(" + whereCondition + " OR " + defaultCollectionCondition + ")";
    }

    return "SELECT idx.* FROM system:indexes AS idx" +
            " WHERE " + whereCondition +
            " AND `using` = \"gsi\"" +
            " ORDER BY is_primary DESC, name ASC";
  }

  public CompletableFuture<Void> createIndex(String indexName,
                                             Collection<String> fields, CoreCreateQueryIndexOptions options) {
    notNullOrEmpty(indexName, "IndexName");
    notNullOrEmpty(fields, "Fields");
    notNull(options, "Options");
    checkScopeAndCollection(options.scopeAndCollection());

    String keyspace = buildKeyspace();
    String statement = "CREATE INDEX " + quote(indexName) + " ON " + keyspace + formatIndexFields(fields);
    Map<String, Object> with = createIndexWith(options);

    return exec(WRITE, statement, with, options.commonOptions(), TracingIdentifiers.SPAN_REQUEST_MQ_CREATE_INDEX, null)
            .exceptionally(t -> {
              if (options.ignoreIfExists() && hasCause(t, IndexExistsException.class)) {
                return null;
              }
              throwIfUnchecked(t);
              throw new RuntimeException(t);
            })
            .thenApply(result -> null);
  }

  public CompletableFuture<Void> createPrimaryIndex(CoreCreatePrimaryQueryIndexOptions options) {
    notNull(options, "Options");
    checkScopeAndCollection(options.scopeAndCollection());

    String keyspace = buildKeyspace();

    String statement = "CREATE PRIMARY INDEX ";
    if (options.indexName() != null) {
      statement += quote(options.indexName()) + " ";
    }
    statement += "ON " + keyspace;
    Map<String, Object> with = createIndexWith(options);

    return exec(WRITE, statement, with, options.commonOptions(), TracingIdentifiers.SPAN_REQUEST_MQ_CREATE_PRIMARY_INDEX, null)
            .exceptionally(t -> {
              if (options.ignoreIfExists() && hasCause(t, IndexExistsException.class)) {
                return null;
              }
              throwIfUnchecked(t);
              throw new RuntimeException(t);
            })
            .thenApply(result -> null);
  }

  public CompletableFuture<List<CoreQueryIndex>> getAllIndexes(CoreGetAllQueryIndexesOptions options) {
    notNull(options, "Options");
    checkScopeAndCollection(options.scopeName(), options.collectionName());

    String statement = getStatementForGetAllIndexes();
    ObjectNode params = getNamedParamsForGetAllIndexes();

    return exec(READ_ONLY, statement, options.commonOptions(), TracingIdentifiers.SPAN_REQUEST_MQ_GET_ALL_INDEXES, params)
            .thenApply(result -> result.rows()
                    .map(CoreQueryIndex::new)
                    .collect(toList()));
  }

  public CompletableFuture<Void> dropPrimaryIndex(CoreDropPrimaryQueryIndexOptions options) {
    notNull(options, "Options");
    checkScopeAndCollection(options.scopeAndCollection());

    String keyspace = buildKeyspace();
    String statement = "DROP PRIMARY INDEX ON " + keyspace;

    return exec(WRITE, statement, options.commonOptions(), TracingIdentifiers.SPAN_REQUEST_MQ_DROP_PRIMARY_INDEX, null)
            .exceptionally(t -> {
              if (options.ignoreIfNotExists() && hasCause(t, IndexNotFoundException.class)) {
                return null;
              }
              throwIfUnchecked(t);
              throw new RuntimeException(t);
            })
            .thenApply(result -> null);
  }

  public CompletableFuture<Void> dropIndex(String indexName,
                                           CoreDropQueryIndexOptions options) {
    notNullOrEmpty(indexName, "IndexName");
    notNull(options, "Options");
    checkScopeAndCollection(options.scopeAndCollection());

    String keyspace = buildKeyspace();
    String statement = "DROP INDEX " + quote(indexName) + " ON " + keyspace;

    return exec(WRITE, statement, options.commonOptions(), TracingIdentifiers.SPAN_REQUEST_MQ_DROP_INDEX, null)
            .exceptionally(t -> {
              if (options.ignoreIfNotExists() && hasCause(t, IndexNotFoundException.class)) {
                return null;
              }
              throwIfUnchecked(t);
              throw new RuntimeException(t);
            })
            .thenApply(result -> null);
  }

  public CompletableFuture<Void> buildDeferredIndexes(CoreBuildQueryIndexOptions options) {
    notNull(options, "Options");
    checkScopeAndCollection(options.scopeAndCollection());

    CoreGetAllQueryIndexesOptions getAllOptions = createGetAllOptions(options.commonOptions());

    return Reactor
            .toMono(() -> getAllIndexes(getAllOptions))
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

              String keyspace = buildKeyspace();

              String statement = "BUILD INDEX ON " + keyspace + " (" + String.join(",", indexNames) + ")";

              return Reactor.toMono(
                      () -> exec(WRITE, statement, options.commonOptions(), TracingIdentifiers.SPAN_REQUEST_MQ_BUILD_DEFERRED_INDEXES, null)
                              .thenApply(result -> null)
              );
            })
            .then()
            .toFuture();
  }

  public CompletableFuture<Void> watchIndexes(Collection<String> indexNames,
                                              Duration timeout, CoreWatchQueryIndexesOptions options) {
    notNull(indexNames, "IndexNames");
    notNull(timeout, "Timeout");
    notNull(options, "Options");
    checkScopeAndCollection(options.scopeAndCollection());

    Set<String> indexNameSet = new HashSet<>(indexNames);

    RequestSpan parent = core.context().environment().requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_MQ_WATCH_INDEXES, null);
    parent.attribute(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);

    return Mono.fromFuture(() -> failIfIndexesOffline(indexNameSet, options.watchPrimary(), parent))
            .retryWhen(Retry.onlyIf(ctx -> hasCause(ctx.exception(), IndexesNotReadyException.class))
                    .exponentialBackoff(Duration.ofMillis(50), Duration.ofSeconds(1))
                    .timeout(timeout)
                    .toReactorRetry())
            .onErrorMap(t -> t instanceof RetryExhaustedException ? toWatchTimeoutException(t, timeout) : t)
            .toFuture()
            .whenComplete((r, t) -> parent.end());
  }

  public static String formatIndexFields(Collection<String> fields) {
    return "(" + String.join(",", fields) + ")";
  }

  public static TimeoutException toWatchTimeoutException(Throwable t, Duration timeout) {
    StringBuilder msg = new StringBuilder("A requested index is still not ready after " + timeout + ".");

    findCause(t, IndexesNotReadyException.class).ifPresent(cause ->
            msg.append(" Unready index name -> state: ").append(redactMeta(cause.indexNameToState())));

    return new TimeoutException(msg.toString());
  }

  private CompletableFuture<Void> failIfIndexesOffline(Set<String> indexNames, boolean includePrimary, RequestSpan parentSpan)
          throws IndexesNotReadyException, IndexNotFoundException {

    requireNonNull(indexNames);

    CoreGetAllQueryIndexesOptions getAllQueryIndexesOptions = createGetAllOptions(CoreCommonOptions.of(null, null, parentSpan));

    return getAllIndexes(getAllQueryIndexesOptions)
            .thenApply(allIndexes -> failIfIndexesOfflineHelper(indexNames, includePrimary, allIndexes));
  }

  public static Void failIfIndexesOfflineHelper(Set<String> indexNames, boolean includePrimary, List<CoreQueryIndex> allIndexes) {
    List<CoreQueryIndex> matchingIndexes = allIndexes.stream()
            .filter(idx -> indexNames.contains(idx.name()) || (includePrimary && idx.primary()))
            .collect(toList());

    boolean primaryIndexPresent = matchingIndexes.stream()
            .anyMatch(CoreQueryIndex::primary);

    if (includePrimary && !primaryIndexPresent) {
      throw new IndexNotFoundException("#primary");
    }

    Set<String> matchingIndexNames = matchingIndexes.stream()
            .map(CoreQueryIndex::name)
            .collect(toSet());

    Set<String> missingIndexNames = difference(indexNames, matchingIndexNames);
    if (!missingIndexNames.isEmpty()) {
      throw new IndexNotFoundException(missingIndexNames.toString());
    }

    Map<String, String> offlineIndexNameToState = matchingIndexes.stream()
            .filter(idx -> !"online".equals(idx.state()))
            .collect(toMap(CoreQueryIndex::name, CoreQueryIndex::state));

    if (!offlineIndexNameToState.isEmpty()) {
      throw new IndexesNotReadyException(offlineIndexNameToState);
    }

    return null;
  }

  /**
   * Returns a set containing all items in {@code lhs} that are not also in {@code rhs}.
   */
  private static <T> Set<T> difference(Set<T> lhs, Set<T> rhs) {
    Set<T> result = new HashSet<>(lhs);
    result.removeAll(rhs);
    return result;
  }

  private CoreGetAllQueryIndexesOptions createGetAllOptions(CoreCommonOptions options) {
    return new CoreGetAllQueryIndexesOptions() {
      @Override
      public String scopeName() {
        return null;
      }

      @Override
      public String collectionName() {
        return null;
      }

      @Override
      public CoreCommonOptions commonOptions() {
        return options;
      }
    };
  }

  private CompletableFuture<CoreQueryResult> exec(CoreQueryType queryType, CharSequence statement, @Nullable Map<String, Object> with,
                                                  CoreCommonOptions options, String spanName, ObjectNode parameters) {
    return (with == null || with.isEmpty())
            ? exec(queryType, statement, options, spanName, parameters)
            : exec(queryType, statement + " WITH " + Mapper.encodeAsString(with), options, spanName, parameters);
  }

  private CompletableFuture<CoreQueryResult> exec(CoreQueryType queryType, CharSequence statement,
                                                  CoreCommonOptions options, String spanName, ObjectNode parameters) {
    RequestSpan parent = core.context().environment().requestTracer().requestSpan(spanName, options.parentSpan().orElse(null));
    parent.attribute(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);

    CoreCommonOptions common = CoreCommonOptions.ofOptional(options.timeout(), options.retryStrategy(), Optional.of(parent));

    CoreQueryOptions queryOpts = toQueryOptions(common, requireNonNull(queryType) == READ_ONLY, parameters);

    parent.attribute(TracingIdentifiers.ATTR_NAME, collection.bucket());
    parent.attribute(TracingIdentifiers.ATTR_SCOPE, collection.scope());
    parent.attribute(TracingIdentifiers.ATTR_COLLECTION, collection.collection());

    return queryOps
            .queryAsync(statement.toString(), queryOpts, queryContext, null, null)
            .toFuture()
            .whenComplete((r, t) -> parent.end());
  }

  public static CoreQueryOptions toQueryOptions(CoreCommonOptions options, boolean readonly, ObjectNode parameters) {
    return new CoreQueryOptions() {
      @Override
      public boolean adhoc() {
        return true;
      }

      @Override
      public String clientContextId() {
        return null;
      }

      @Override
      public CoreMutationState consistentWith() {
        return null;
      }

      @Override
      public Integer maxParallelism() {
        return null;
      }

      @Override
      public boolean metrics() {
        return false;
      }

      @Override
      public ObjectNode namedParameters() {
        return parameters;
      }

      @Override
      public Integer pipelineBatch() {
        return null;
      }

      @Override
      public Integer pipelineCap() {
        return null;
      }

      @Override
      public ArrayNode positionalParameters() {
        return null;
      }

      @Override
      public CoreQueryProfile profile() {
        return null;
      }

      @Override
      public JsonNode raw() {
        return null;
      }

      @Override
      public boolean readonly() {
        return readonly;
      }

      @Override
      public Duration scanWait() {
        return null;
      }

      @Override
      public Integer scanCap() {
        return null;
      }

      @Override
      public CoreQueryScanConsistency scanConsistency() {
        return null;
      }

      @Override
      public boolean flexIndex() {
        return false;
      }

      @Override
      public Boolean preserveExpiry() {
        return null;
      }

      @Override
      public CoreSingleQueryTransactionOptions asTransactionOptions() {
        return null;
      }

      @Override
      public CoreCommonOptions commonOptions() {
        return options;
      }
    };
  }

  private static String quote(String s) {
    if (s.contains("`")) {
      throw InvalidArgumentException.fromMessage("Value [" + redactMeta(s) + "] may not contain backticks.");
    }
    return "`" + s + "`";
  }

  public static String quote(String... components) {
    return Arrays.stream(components)
            .map(CoreCollectionQueryIndexManager::quote)
            .collect(Collectors.joining("."));
  }

  public static String quote(CoreKeyspace keyspace) {
    return quote(keyspace.bucket()) + "." + quote(keyspace.scope()) + "." + quote(keyspace.collection());
  }

  private String buildKeyspace() {
    return quote(collection);
  }

  private void checkScopeAndCollection(@Nullable CoreScopeAndCollection scopeAndCollection) {
    if (scopeAndCollection != null) {
      throw InvalidArgumentException.fromMessage("scopeName and collectionName should not be used together with CollectionQueryIndexManager, which is already acting on a particular Collection");
    }
  }

  private void checkScopeAndCollection(@Nullable String scopeName, @Nullable String collectionName) {
    if (scopeName != null || collectionName != null) {
      throw InvalidArgumentException.fromMessage("scopeName and collectionName should not be used together with CollectionQueryIndexManager, which is already acting on a particular Collection");
    }
  }

  public @Nullable static Map<String, Object> createIndexWith(CoreCreateQueryIndexSharedOptions options) {
    Map<String, Object> with = new HashMap<>();
    if (options.with() != null) {
      with.putAll(options.with());
    }
    if (options.numReplicas() != null) {
      if (options.numReplicas() < 0) {
        throw InvalidArgumentException.fromMessage("numReplicas must be >= 0");
      }
      with.put("num_replica", options.numReplicas());
    }
    if (options.deferred() != null) {
      with.put("defer_build", options.deferred());
    }
    if (with.isEmpty()) {
      return null;
    }
    return with;
  }
}

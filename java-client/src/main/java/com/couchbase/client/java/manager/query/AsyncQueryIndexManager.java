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

import com.couchbase.client.core.error.QueryException;
import com.couchbase.client.core.error.QueryIndexExistsException;
import com.couchbase.client.core.error.QueryIndexNotFoundException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.retry.reactor.Retry;
import com.couchbase.client.core.retry.reactor.RetryExhaustedException;
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.CommonOptions;
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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class AsyncQueryIndexManager {
  enum QueryType {
    READ_ONLY,
    WRITE
  }

  private final AsyncCluster cluster;

  public AsyncQueryIndexManager(AsyncCluster cluster) {
    this.cluster = requireNonNull(cluster);
  }

  public CompletableFuture<Void> createIndex(String bucketName, String indexName, Collection<String> fields) {
    return createIndex(bucketName, indexName, fields, createQueryIndexOptions());
  }

  public CompletableFuture<Void> createIndex(String bucketName, String indexName, Collection<String> fields, CreateQueryIndexOptions options) {
    final CreateQueryIndexOptions.Built builtOpts = options.build();

    String statement = "CREATE INDEX " + quote(indexName) + " ON " + quote(bucketName) + formatIndexFields(fields);

    return exec(WRITE, statement, builtOpts.with(), builtOpts)
        .exceptionally(t -> {
          if (builtOpts.ignoreIfExists() && hasCause(t, QueryIndexExistsException.class)) {
            return null;
          }
          throwIfUnchecked(t);
          throw new RuntimeException(t);
        })
        .thenApply(result -> null);
  }

  public CompletableFuture<Void> createPrimaryIndex(String bucketName) {
    return createPrimaryIndex(bucketName, createPrimaryQueryIndexOptions());
  }

  public CompletableFuture<Void> createPrimaryIndex(String bucketName, CreatePrimaryQueryIndexOptions options) {
    final CreatePrimaryQueryIndexOptions.Built builtOpts = options.build();
    final String indexName = builtOpts.indexName().orElse(null);

    String statement = "CREATE PRIMARY INDEX ";
    if (indexName != null) {
      statement += quote(indexName) + " ";
    }
    statement += "ON " + quote(bucketName);

    return exec(WRITE, statement, builtOpts.with(), builtOpts)
        .exceptionally(t -> {
          if (builtOpts.ignoreIfExists() && hasCause(t, QueryIndexExistsException.class)) {
            return null;
          }
          throwIfUnchecked(t);
          throw new RuntimeException(t);
        })
        .thenApply(result -> null);
  }

  private static String formatIndexFields(Collection<String> fields) {
    return "(" + String.join(",", fields) + ")";
  }

  public CompletableFuture<List<QueryIndex>> getAllIndexes(String bucketName) {
    return getAllIndexes(bucketName, getAllQueryIndexesOptions());
  }

  public CompletableFuture<List<QueryIndex>> getAllIndexes(String bucketName, GetAllQueryIndexesOptions options) {
    requireNonNull(bucketName);

    final GetAllQueryIndexesOptions.Built builtOpts = options.build();

    String statement = "SELECT idx.* FROM system:indexes AS idx" +
        " WHERE keyspace_id = \"" + bucketName + "\"" +
        " ORDER BY is_primary DESC, name ASC";

    return exec(READ_ONLY, statement, builtOpts)
        .thenApply(result -> result.rowsAsObject().stream()
            .map(QueryIndex::new)
            .collect(toList()));
  }

  public CompletableFuture<Void> dropPrimaryIndex(String bucketName) {
    return dropPrimaryIndex(bucketName, dropPrimaryQueryIndexOptions());
  }

  public CompletableFuture<Void> dropPrimaryIndex(String bucketName, DropPrimaryQueryIndexOptions options) {
    requireNonNull(bucketName);

    final DropPrimaryQueryIndexOptions.Built builtOpts = options.build();

    String statement = "DROP PRIMARY INDEX ON " + quote(bucketName);

    return exec(WRITE, statement, builtOpts)
        .exceptionally(t -> {
          if (builtOpts.ignoreIfNotExists() && hasCause(t, QueryIndexNotFoundException.class)) {
            return null;
          }
          throwIfUnchecked(t);
          throw new RuntimeException(t);
        })
        .thenApply(result -> null);
  }

  public CompletableFuture<Void> dropIndex(String bucketName, String indexName) {
    return dropIndex(bucketName, indexName, dropQueryIndexOptions());
  }

  public CompletableFuture<Void> dropIndex(String bucketName, String indexName, DropQueryIndexOptions options) {
    requireNonNull(bucketName);

    final DropQueryIndexOptions.Built builtOpts = options.build();

    String statement = "DROP INDEX " + quote(bucketName, indexName);

    return exec(WRITE, statement, builtOpts)
        .exceptionally(t -> {
          if (builtOpts.ignoreIfNotExists() && hasCause(t, QueryIndexNotFoundException.class)) {
            return null;
          }
          throwIfUnchecked(t);
          throw new RuntimeException(t);
        })
        .thenApply(result -> null);
  }

  public CompletableFuture<Void> buildDeferredIndexes(String bucketName) {
    return buildDeferredIndexes(bucketName, buildDeferredQueryIndexesOptions());
  }

  private static GetAllQueryIndexesOptions toGetAllIndexesOptions(CommonOptions<?>.BuiltCommonOptions opts) {
    GetAllQueryIndexesOptions result = getAllQueryIndexesOptions();
    opts.retryStrategy().ifPresent(result::retryStrategy);
    opts.timeout().ifPresent(result::timeout);
    return result;
  }

  public CompletableFuture<Void> buildDeferredIndexes(String bucketName, BuildQueryIndexOptions options) {
    requireNonNull(bucketName);
    final BuildQueryIndexOptions.Built builtOpts = options.build();

    return getAllIndexes(bucketName, toGetAllIndexesOptions(builtOpts))
        .thenCompose(allIndexes -> {
          List<String> deferredIndexNames = allIndexes.stream()
              .filter(idx -> "deferred".equals(idx.state()))
              .map(QueryIndex::name)
              .collect(toList());

          if (deferredIndexNames.isEmpty()) {
            return completedFuture(null);
          }

          String statement = "BUILD INDEX ON " + quote(bucketName) + "(" +
              deferredIndexNames.stream()
                  .map(AsyncQueryIndexManager::quote)
                  .collect(Collectors.joining(",")) + ")";

          return exec(WRITE, statement, builtOpts);

        })
        .thenApply(result -> null);
  }

  public CompletableFuture<Void> watchIndexes(String bucketName, Collection<String> indexNames, Duration timeout) {
    return watchIndexes(bucketName, indexNames, timeout, watchQueryIndexesOptions());
  }

  public CompletableFuture<Void> watchIndexes(String bucketName, Collection<String> indexNames, Duration timeout, WatchQueryIndexesOptions options) {
    requireNonNull(timeout);

    Set<String> indexNameSet = new HashSet<>(indexNames);
    WatchQueryIndexesOptions.Built builtOpts = options.build();

    return Mono.fromFuture(() -> failIfIndexesOffline(bucketName, indexNameSet, builtOpts.watchPrimary()))
        .retryWhen(Retry.onlyIf(ctx -> hasCause(ctx.exception(), IndexesNotReadyException.class))
            .exponentialBackoff(Duration.ofMillis(50), Duration.ofSeconds(1))
            .timeout(timeout))
        .onErrorMap(t -> t instanceof RetryExhaustedException ? toWatchTimeoutException(t, timeout) : t)
        .toFuture();
  }

  private static TimeoutException toWatchTimeoutException(Throwable t, Duration timeout) {
    final StringBuilder msg = new StringBuilder("A requested index is still not ready after " + timeout + ".");

    findCause(t, IndexesNotReadyException.class).ifPresent(cause ->
        msg.append(" Unready index name -> state: ").append(redactMeta(cause.indexNameToState())));

    return new TimeoutException(msg.toString());
  }

  private CompletableFuture<Void> failIfIndexesOffline(String bucketName,
                                                       Set<String> indexNames,
                                                       boolean includePrimary)
      throws IndexesNotReadyException, QueryIndexNotFoundException {

    requireNonNull(bucketName);
    requireNonNull(indexNames);

    return getAllIndexes(bucketName)
        .thenApply(allIndexes -> {
          final List<QueryIndex> matchingIndexes = allIndexes.stream()
              .filter(idx -> indexNames.contains(idx.name()) || (includePrimary && idx.primary()))
              .collect(toList());

          final boolean primaryIndexPresent = matchingIndexes.stream()
              .anyMatch(QueryIndex::primary);

          if (includePrimary && !primaryIndexPresent) {
            throw new QueryIndexNotFoundException("#primary");
          }

          final Set<String> matchingIndexNames = matchingIndexes.stream()
              .map(QueryIndex::name)
              .collect(toSet());

          final Set<String> missingIndexNames = difference(indexNames, matchingIndexNames);
          if (!missingIndexNames.isEmpty()) {
            throw new QueryIndexNotFoundException(missingIndexNames.toString());
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

  private CompletableFuture<QueryResult> exec(QueryType queryType, CharSequence statement, Map<String, Object> with, CommonOptions<?>.BuiltCommonOptions options) {
    return with.isEmpty()
        ? exec(queryType, statement, options)
        : exec(queryType, statement + " WITH " + Mapper.encodeAsString(with), options);
  }

  private CompletableFuture<QueryResult> exec(QueryType queryType, CharSequence statement, CommonOptions<?>.BuiltCommonOptions options) {
    QueryOptions queryOpts = toQueryOptions(options)
        .readonly(requireNonNull(queryType) == READ_ONLY);

    return cluster.query(statement.toString(), queryOpts)
        .exceptionally(t -> {
          throw translateException(t);
        });
  }

  private static QueryOptions toQueryOptions(CommonOptions<?>.BuiltCommonOptions options) {
    QueryOptions result = QueryOptions.queryOptions();
    options.timeout().ifPresent(result::timeout);
    options.retryStrategy().ifPresent(result::retryStrategy);
    return result;
  }

  private static final Map<Predicate<QueryException>, Function<QueryException, ? extends QueryException>> errorMessageMap = new LinkedHashMap<>();

  private static Predicate<QueryException> code(int code) {
    return e -> e.code() == code;
  }

  private static Predicate<QueryException> message(String substringRegex) {
    final String CASE_INSENSITIVE = "(?i)";
    return e -> e.msg().matches(CASE_INSENSITIVE + ".*\\b" + substringRegex + "\\b.*");
  }

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
      throw new IllegalArgumentException("Value [" + redactMeta(s) + "] may not contain backticks.");
    }
    return "`" + s + "`";
  }

  private static String quote(String... components) {
    return Arrays.stream(components)
        .map(AsyncQueryIndexManager::quote)
        .collect(Collectors.joining("."));
  }
}

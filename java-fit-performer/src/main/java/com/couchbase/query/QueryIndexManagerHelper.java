/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.query;

// [skip:<3.4.3]

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.manager.query.BuildQueryIndexOptions;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.manager.query.CreateQueryIndexOptions;
import com.couchbase.client.java.manager.query.DropPrimaryQueryIndexOptions;
import com.couchbase.client.java.manager.query.DropQueryIndexOptions;
import com.couchbase.client.java.manager.query.GetAllQueryIndexesOptions;
import com.couchbase.client.java.manager.query.QueryIndex;
import com.couchbase.client.java.manager.query.WatchQueryIndexesOptions;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.query.indexmanager.QueryIndexType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.couchbase.JavaSdkCommandExecutor.setSuccess;
import static com.couchbase.client.performer.core.util.TimeUtil.getTimeNow;

public class QueryIndexManagerHelper {
  private QueryIndexManagerHelper() {
  }

  public static void handleClusterQueryIndexManager(Cluster cluster,
                                                    ConcurrentHashMap<String, RequestSpan> spans,
                                                    com.couchbase.client.protocol.sdk.Command command,
                                                    Result.Builder result) {
    var qim = command.getClusterCommand().getQueryIndexManager();

    if (!qim.hasShared()) {
      throw new UnsupportedOperationException();
    }

    var op = command.getClusterCommand().getQueryIndexManager().getShared();
    handleQueryIndexManagerShared(cluster, qim.getBucketName(), null, spans, command, op, result);
  }

  public static void handleCollectionQueryIndexManager(Collection collection,
                                                       ConcurrentHashMap<String, RequestSpan> spans,
                                                       com.couchbase.client.protocol.sdk.Command command,
                                                       Result.Builder result) {
    if (!command.getCollectionCommand().getQueryIndexManager().hasShared()) {
      throw new UnsupportedOperationException();
    }

    var op = command.getCollectionCommand().getQueryIndexManager().getShared();
    handleQueryIndexManagerShared(null, null, collection, spans, command, op, result);

        /*
    throw new UnsupportedOperationException();
        */
  }

  public static Mono<Result> handleClusterQueryIndexManagerReactive(ReactiveCluster cluster,
                                                                    ConcurrentHashMap<String, RequestSpan> spans,
                                                                    com.couchbase.client.protocol.sdk.Command command,
                                                                    Result.Builder result) {
    var qim = command.getClusterCommand().getQueryIndexManager();

    if (!qim.hasShared()) {
      throw new UnsupportedOperationException();
    }

    var op = command.getClusterCommand().getQueryIndexManager().getShared();
    return handleQueryIndexManagerSharedReactive(cluster, qim.getBucketName(), null, spans, command, op, result);
  }

  public static Mono<Result> handleCollectionQueryIndexManagerReactive(ReactiveCollection collection,
                                                                       ConcurrentHashMap<String, RequestSpan> spans,
                                                                       com.couchbase.client.protocol.sdk.Command command,
                                                                       Result.Builder result) {
    if (!command.getCollectionCommand().getQueryIndexManager().hasShared()) {
      throw new UnsupportedOperationException();
    }

    var op = command.getCollectionCommand().getQueryIndexManager().getShared();
    return handleQueryIndexManagerSharedReactive(null, null, collection, spans, command, op, result);
  }

  /**
   * Used for both QueryIndexManager (`bucketName` and `cluster` will not be null) and
   * CollectionQueryIndexManager (`collection` will not be null)
   */
  private static void handleQueryIndexManagerShared(@Nullable Cluster cluster,
                                                    @Nullable String bucketName,
                                                    @Nullable Collection collection,
                                                    ConcurrentHashMap<String, RequestSpan> spans,
                                                    com.couchbase.client.protocol.sdk.Command command,
                                                    com.couchbase.client.protocol.sdk.query.indexmanager.Command op,
                                                    Result.Builder result) {

    if (op.hasCreatePrimaryIndex()) {
      var request = op.getCreatePrimaryIndex();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (collection == null) {
        if (options == null) {
          cluster.queryIndexes().createPrimaryIndex(bucketName);
        } else {
          cluster.queryIndexes().createPrimaryIndex(bucketName, options);
        }
      } else {
        if (options == null) {
          collection.queryIndexes().createPrimaryIndex();
        } else {
          collection.queryIndexes().createPrimaryIndex(options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    } else if (op.hasCreateIndex()) {
      var request = op.getCreateIndex();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      var fields = request.getFieldsList().stream().map(f -> f.toString()).collect(Collectors.toSet());
      if (collection == null) {
        if (options == null) {
          cluster.queryIndexes().createIndex(bucketName, request.getIndexName(), fields);
        } else {
          cluster.queryIndexes().createIndex(bucketName, request.getIndexName(), fields, options);
        }
      } else {
        if (options == null) {
          collection.queryIndexes().createIndex(request.getIndexName(), fields);
        } else {
          collection.queryIndexes().createIndex(request.getIndexName(), fields, options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    } else if (op.hasGetAllIndexes()) {
      var request = op.getGetAllIndexes();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      List<QueryIndex> indexes;
      if (collection == null) {
        if (options == null) {
          indexes = cluster.queryIndexes().getAllIndexes(bucketName);
        } else {
          indexes = cluster.queryIndexes().getAllIndexes(bucketName, options);
        }
      } else {
        if (options == null) {
          indexes = collection.queryIndexes().getAllIndexes();
        } else {
          indexes = collection.queryIndexes().getAllIndexes(options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      if (command.getReturnResult()) {
        populateResult(result, indexes);
      } else {
        setSuccess(result);
      }
    } else if (op.hasDropPrimaryIndex()) {
      var request = op.getDropPrimaryIndex();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (collection == null) {
        if (options == null) {
          cluster.queryIndexes().dropPrimaryIndex(bucketName);
        } else {
          cluster.queryIndexes().dropPrimaryIndex(bucketName, options);
        }
      } else {
        if (options == null) {
          collection.queryIndexes().dropPrimaryIndex();
        } else {
          collection.queryIndexes().dropPrimaryIndex(options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    } else if (op.hasDropIndex()) {
      var request = op.getDropIndex();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (collection == null) {
        if (options == null) {
          cluster.queryIndexes().dropIndex(bucketName, request.getIndexName());
        } else {
          cluster.queryIndexes().dropIndex(bucketName, request.getIndexName(), options);
        }
      } else {
        if (options == null) {
          collection.queryIndexes().dropIndex(request.getIndexName());
        } else {
          collection.queryIndexes().dropIndex(request.getIndexName(), options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    } else if (op.hasWatchIndexes()) {
      var request = op.getWatchIndexes();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (collection == null) {
        if (options == null) {
          cluster.queryIndexes().watchIndexes(bucketName, request.getIndexNamesList().stream().toList(), Duration.ofMillis(request.getTimeoutMsecs()));
        } else {
          cluster.queryIndexes().watchIndexes(bucketName, request.getIndexNamesList().stream().toList(), Duration.ofMillis(request.getTimeoutMsecs()), options);
        }
      } else {
        if (options == null) {
          collection.queryIndexes().watchIndexes(request.getIndexNamesList().stream().toList(), Duration.ofMillis(request.getTimeoutMsecs()));
        } else {
          collection.queryIndexes().watchIndexes(request.getIndexNamesList().stream().toList(), Duration.ofMillis(request.getTimeoutMsecs()), options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    } else if (op.hasBuildDeferredIndexes()) {
      var request = op.getBuildDeferredIndexes();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (collection == null) {
        if (options == null) {
          cluster.queryIndexes().buildDeferredIndexes(bucketName);
        } else {
          cluster.queryIndexes().buildDeferredIndexes(bucketName, options);
        }
      } else {
        if (options == null) {
          collection.queryIndexes().buildDeferredIndexes();
        } else {
          collection.queryIndexes().buildDeferredIndexes(options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    } else {
      throw new UnsupportedOperationException();
    }
  }


  /**
   * Used for both QueryIndexManager (`bucketName` and `cluster` will not be null) and
   * CollectionQueryIndexManager (`collection` will not be null)
   */
  private static Mono<Result> handleQueryIndexManagerSharedReactive(@Nullable ReactiveCluster cluster,
                                                                    @Nullable String bucketName,
                                                                    @Nullable ReactiveCollection collection,
                                                                    ConcurrentHashMap<String, RequestSpan> spans,
                                                                    com.couchbase.client.protocol.sdk.Command op,
                                                                    com.couchbase.client.protocol.sdk.query.indexmanager.Command command,
                                                                    Result.Builder result) {
    if (command.hasCreatePrimaryIndex()) {
      var request = command.getCreatePrimaryIndex();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      Mono<Void> res;
      if (collection == null) {
        if (options == null) {
          res = cluster.queryIndexes().createPrimaryIndex(bucketName);
        } else {
          res = cluster.queryIndexes().createPrimaryIndex(bucketName, options);
        }
      } else {
        if (options == null) {
          res = collection.queryIndexes().createPrimaryIndex();
        } else {
          res = collection.queryIndexes().createPrimaryIndex(options);
        }
      }
      return res.then(Mono.fromCallable(() -> {
        result.setElapsedNanos(System.nanoTime() - start);
        setSuccess(result);
        return result.build();
      }));
    } else if (command.hasCreateIndex()) {
      var request = command.getCreateIndex();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      var fields = new HashSet<>(request.getFieldsList());
      Mono<Void> res;
      if (collection == null) {
        if (options == null) {
          res = cluster.queryIndexes().createIndex(bucketName, request.getIndexName(), fields);
        } else {
          res = cluster.queryIndexes().createIndex(bucketName, request.getIndexName(), fields, options);
        }
      } else {
        if (options == null) {
          res = collection.queryIndexes().createIndex(request.getIndexName(), fields);
        } else {
          res = collection.queryIndexes().createIndex(request.getIndexName(), fields, options);
        }
      }
      return res.then(Mono.fromCallable(() -> {
        result.setElapsedNanos(System.nanoTime() - start);
        setSuccess(result);
        return result.build();
      }));
    } else if (command.hasGetAllIndexes()) {
      var request = command.getGetAllIndexes();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      Flux<QueryIndex> indexes;
      if (collection == null) {
        if (options == null) {
          indexes = cluster.queryIndexes().getAllIndexes(bucketName);
        } else {
          indexes = cluster.queryIndexes().getAllIndexes(bucketName, options);
        }
      } else {
        if (options == null) {
          indexes = collection.queryIndexes().getAllIndexes();
        } else {
          indexes = collection.queryIndexes().getAllIndexes(options);
        }
      }
      return indexes.collectList().map(i -> {
        result.setElapsedNanos(System.nanoTime() - start);
        if (op.getReturnResult()) {
          populateResult(result, i);
        } else {
          setSuccess(result);
        }
        return result.build();
      });
    } else if (command.hasDropPrimaryIndex()) {
      var request = command.getDropPrimaryIndex();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      Mono<Void> res;
      if (collection == null) {
        if (options == null) {
          res = cluster.queryIndexes().dropPrimaryIndex(bucketName);
        } else {
          res = cluster.queryIndexes().dropPrimaryIndex(bucketName, options);
        }
      } else {
        if (options == null) {
          res = collection.queryIndexes().dropPrimaryIndex();
        } else {
          res = collection.queryIndexes().dropPrimaryIndex(options);
        }
      }
      return res.then(Mono.fromCallable(() -> {
        result.setElapsedNanos(System.nanoTime() - start);
        setSuccess(result);
        return result.build();
      }));
    } else if (command.hasDropIndex()) {
      var request = command.getDropIndex();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      Mono<Void> res;
      if (collection == null) {
        if (options == null) {
          res = cluster.queryIndexes().dropIndex(bucketName, request.getIndexName());
        } else {
          res = cluster.queryIndexes().dropIndex(bucketName, request.getIndexName(), options);
        }
      } else {
        if (options == null) {
          res = collection.queryIndexes().dropIndex(request.getIndexName());
        } else {
          res = collection.queryIndexes().dropIndex(request.getIndexName(), options);
        }
      }
      return res.then(Mono.fromCallable(() -> {
        result.setElapsedNanos(System.nanoTime() - start);
        setSuccess(result);
        return result.build();
      }));
    } else if (command.hasWatchIndexes()) {
      var request = command.getWatchIndexes();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      Mono<Void> res;
      if (collection == null) {
        if (options == null) {
          res = cluster.queryIndexes().watchIndexes(bucketName, request.getIndexNamesList().stream().toList(), Duration.ofMillis(request.getTimeoutMsecs()));
        } else {
          res = cluster.queryIndexes().watchIndexes(bucketName, request.getIndexNamesList().stream().toList(), Duration.ofMillis(request.getTimeoutMsecs()), options);
        }
      } else {
        if (options == null) {
          res = collection.queryIndexes().watchIndexes(request.getIndexNamesList().stream().toList(), Duration.ofMillis(request.getTimeoutMsecs()));
        } else {
          res = collection.queryIndexes().watchIndexes(request.getIndexNamesList().stream().toList(), Duration.ofMillis(request.getTimeoutMsecs()), options);
        }
      }
      return res.then(Mono.fromCallable(() -> {
        result.setElapsedNanos(System.nanoTime() - start);
        setSuccess(result);
        return result.build();
      }));
    } else if (command.hasBuildDeferredIndexes()) {
      var request = command.getBuildDeferredIndexes();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      Mono<Void> res;
      if (collection == null) {
        if (options == null) {
          res = cluster.queryIndexes().buildDeferredIndexes(bucketName);
        } else {
          res = cluster.queryIndexes().buildDeferredIndexes(bucketName, options);
        }
      } else {
        if (options == null) {
          res = collection.queryIndexes().buildDeferredIndexes();
        } else {
          res = collection.queryIndexes().buildDeferredIndexes(options);
        }
      }
      return res.then(Mono.fromCallable(() -> {
        result.setElapsedNanos(System.nanoTime() - start);
        setSuccess(result);
        return result.build();
      }));
    } else {
      return Mono.error(new UnsupportedOperationException());
    }
  }

  public static void populateResult(com.couchbase.client.protocol.run.Result.Builder result, List<QueryIndex> indexes) {
    var builder = com.couchbase.client.protocol.sdk.query.indexmanager.QueryIndexes.newBuilder();
    for (QueryIndex idx : indexes) {
      var index = com.couchbase.client.protocol.sdk.query.indexmanager.QueryIndex.newBuilder()
              .setName(idx.name())
              .setIsPrimary(idx.primary())
              .setState(idx.state())
              .setKeyspace(idx.keyspace())
              .setType(QueryIndexType.valueOf(idx.type().toUpperCase()));

      // bucketName only added in 3.2.5
      index.setBucketName(idx.bucketName());

      if (idx.scopeName().isPresent()) {
        index.setScopeName(idx.scopeName().get());
      }

      if (idx.collectionName().isPresent()) {
        index.setCollectionName(idx.collectionName().get());
      }

      idx.indexKey().forEach(k -> index.addIndexKey((String) k));
      if (idx.condition().isPresent()) index.setCondition(idx.condition().get());
      if (idx.partition().isPresent()) index.setPartition(idx.partition().get());

      builder.addIndexes(index);
    }
    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
            .setQueryIndexes(builder));
  }

  public static @Nullable CreatePrimaryQueryIndexOptions createOptions(com.couchbase.client.protocol.sdk.query.indexmanager.CreatePrimaryIndex request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (request.hasOptions()) {
      var opts = request.getOptions();
      var out = CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions();
      if (opts.hasIgnoreIfExists()) out.ignoreIfExists(opts.getIgnoreIfExists());
      if (opts.hasDeferred()) out.deferred(opts.getDeferred());
      if (opts.hasIndexName()) out.indexName(opts.getIndexName());
      if (opts.hasNumReplicas()) out.numReplicas(opts.getNumReplicas());
      if (opts.hasScopeName()) out.scopeName(opts.getScopeName());
      if (opts.hasCollectionName()) out.collectionName(opts.getCollectionName());
      if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
      if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
      return out;
    } else {
      return null;
    }
  }

  public static @Nullable CreateQueryIndexOptions createOptions(com.couchbase.client.protocol.sdk.query.indexmanager.CreateIndex request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (request.hasOptions()) {
      var opts = request.getOptions();
      var out = CreateQueryIndexOptions.createQueryIndexOptions();
      if (opts.hasIgnoreIfExists()) out.ignoreIfExists(opts.getIgnoreIfExists());
      if (opts.hasDeferred()) out.deferred(opts.getDeferred());
      if (opts.hasNumReplicas()) out.numReplicas(opts.getNumReplicas());
      if (opts.hasScopeName()) out.scopeName(opts.getScopeName());
      if (opts.hasCollectionName()) out.collectionName(opts.getCollectionName());
      if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
      if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
      return out;
    } else {
      return null;
    }
  }

  public static @Nullable DropPrimaryQueryIndexOptions createOptions(com.couchbase.client.protocol.sdk.query.indexmanager.DropPrimaryIndex request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (request.hasOptions()) {
      var opts = request.getOptions();
      var out = DropPrimaryQueryIndexOptions.dropPrimaryQueryIndexOptions();
      if (opts.hasIgnoreIfNotExists()) out.ignoreIfNotExists(opts.getIgnoreIfNotExists());
      if (opts.hasScopeName()) out.scopeName(opts.getScopeName());
      if (opts.hasCollectionName()) out.collectionName(opts.getCollectionName());
      if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
      if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
      return out;
    } else {
      return null;
    }
  }

  public static @Nullable DropQueryIndexOptions createOptions(com.couchbase.client.protocol.sdk.query.indexmanager.DropIndex request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (request.hasOptions()) {
      var opts = request.getOptions();
      var out = DropQueryIndexOptions.dropQueryIndexOptions();
      if (opts.hasIgnoreIfNotExists()) out.ignoreIfNotExists(opts.getIgnoreIfNotExists());
      if (opts.hasScopeName()) out.scopeName(opts.getScopeName());
      if (opts.hasCollectionName()) out.collectionName(opts.getCollectionName());
      if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
      if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
      return out;
    } else {
      return null;
    }
  }

  public static @Nullable WatchQueryIndexesOptions createOptions(com.couchbase.client.protocol.sdk.query.indexmanager.WatchIndexes request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (request.hasOptions()) {
      var opts = request.getOptions();
      var out = WatchQueryIndexesOptions.watchQueryIndexesOptions();
      if (opts.hasWatchPrimary()) out.watchPrimary(opts.getWatchPrimary());
      if (opts.hasScopeName()) out.scopeName(opts.getScopeName());
      if (opts.hasCollectionName()) out.collectionName(opts.getCollectionName());
      // [start:3.4.3]
      if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
      // [end:3.4.3]
      return out;
    } else {
      return null;
    }
  }

  public static @Nullable BuildQueryIndexOptions createOptions(com.couchbase.client.protocol.sdk.query.indexmanager.BuildDeferredIndexes request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (request.hasOptions()) {
      var opts = request.getOptions();
      var out = BuildQueryIndexOptions.buildDeferredQueryIndexesOptions();
      if (opts.hasScopeName()) out.scopeName(opts.getScopeName());
      if (opts.hasCollectionName()) out.collectionName(opts.getCollectionName());
      if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
      if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
      return out;
    } else {
      return null;
    }
  }

  public static @Nullable GetAllQueryIndexesOptions createOptions(com.couchbase.client.protocol.sdk.query.indexmanager.GetAllIndexes request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (request.hasOptions()) {
      var opts = request.getOptions();
      var out = GetAllQueryIndexesOptions.getAllQueryIndexesOptions();
      if (opts.hasScopeName()) out.scopeName(opts.getScopeName());
      if (opts.hasCollectionName()) out.collectionName(opts.getCollectionName());
      if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
      if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
      return out;
    } else {
      return null;
    }
  }
}

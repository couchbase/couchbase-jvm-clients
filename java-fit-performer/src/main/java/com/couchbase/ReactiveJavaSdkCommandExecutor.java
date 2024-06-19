/*
 * Copyright 2022 Couchbase, Inc.
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
package com.couchbase;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.protocol.sdk.Command;
import com.couchbase.utils.ContentAsUtil;
import static com.couchbase.JavaSdkCommandExecutor.*;
import static com.couchbase.client.protocol.streams.Type.STREAM_KV_GET_ALL_REPLICAS;
// [if:3.2.1]
import com.couchbase.eventing.EventingHelper;
// [end]
// [if:3.2.4]
import com.couchbase.manager.BucketManagerHelper;
// [end]
// [if:3.4.12]
import com.couchbase.manager.CollectionManagerHelper;
// [end]
// [if:3.4.1]
import com.couchbase.client.java.kv.ScanResult;
import static com.couchbase.JavaSdkCommandExecutor.convertScanType;
import static com.couchbase.JavaSdkCommandExecutor.processScanResult;
// [end]
import com.couchbase.client.performer.core.commands.SdkCommandExecutor;
import com.couchbase.client.performer.core.perf.Counters;
import com.couchbase.client.performer.core.perf.PerRun;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.shared.Exception;
// [if:3.4.3]
import com.couchbase.query.QueryIndexManagerHelper;
// [end]
// [if:3.4.5]
import com.couchbase.search.SearchHelper;

import static com.couchbase.search.SearchHelper.handleSearchQueryReactive;
// [end]
import com.couchbase.stream.FluxStreamer;
import com.couchbase.utils.ClusterConnection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.performer.core.util.TimeUtil.getTimeNow;
import static com.couchbase.client.protocol.streams.Type.STREAM_KV_RANGE_SCAN;
// [if:3.6.0]
import static com.couchbase.search.SearchHelper.handleSearchReactive;
// [end]


/**
 * SdkOperation performs each requested SDK operation
 */
public class ReactiveJavaSdkCommandExecutor extends SdkCommandExecutor {
    private final ClusterConnection connection;
    private final ConcurrentHashMap<String, RequestSpan> spans;

    public ReactiveJavaSdkCommandExecutor(ClusterConnection connection, Counters counters, ConcurrentHashMap<String, RequestSpan> spans) {
        super(counters);
        this.connection = connection;
        this.spans = spans;
    }

    @Override
    protected void performOperation(com.couchbase.client.protocol.sdk.Command op, PerRun perRun) {
        performOperationReactive(op, perRun)
                // A few operations (such as streaming) will intentionally not return a result, so they guarantee
                // the stream.Created is sent first.
                .doOnNext(result -> perRun.resultsStream().enqueue(result))
                .block();
    }

    private Mono<Result> performOperationReactive(com.couchbase.client.protocol.sdk.Command op, PerRun perRun) {
        return Mono.defer(() -> {
            var result = com.couchbase.client.protocol.run.Result.newBuilder();

            if (op.hasInsert()) {
                var request = op.getInsert();
                var collection = connection.collection(request.getLocation()).reactive();
                var content = content(request.getContent());
                var docId = getDocId(request.getLocation());
                var options = createOptions(request, spans);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                Mono<MutationResult> mr;
                if (options == null) mr = collection.insert(docId, content);
                else mr = collection.insert(docId, content, options);
                return mr.map(r -> {
                    result.setElapsedNanos(System.nanoTime() - start);
                    if (op.getReturnResult()) populateResult(result, r);
                    else setSuccess(result);
                    return result.build();
                });
            } else if (op.hasGet()) {
                var request = op.getGet();
                var collection = connection.collection(request.getLocation()).reactive();
                var docId = getDocId(request.getLocation());
                var options = createOptions(request, spans);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                Mono<GetResult> gr;
                if (options == null) gr = collection.get(docId);
                else gr = collection.get(docId, options);
                return gr.map(r -> {
                    result.setElapsedNanos(System.nanoTime() - start);
                    if (op.getReturnResult()) populateResult(request.getContentAs(), result, r);
                    else setSuccess(result);
                    return result.build();
                });
            } else if (op.hasRemove()) {
                var request = op.getRemove();
                var collection = connection.collection(request.getLocation()).reactive();
                var docId = getDocId(request.getLocation());
                var options = createOptions(request, spans);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                Mono<MutationResult> mr;
                if (options == null) mr = collection.remove(docId);
                else mr = collection.remove(docId, options);
                result.setElapsedNanos(System.nanoTime() - start);
                return mr.map(r -> {
                    if (op.getReturnResult()) populateResult(result, r);
                    else setSuccess(result);
                    return result.build();
                });
            } else if (op.hasReplace()) {
                var request = op.getReplace();
                var collection = connection.collection(request.getLocation()).reactive();
                var docId = getDocId(request.getLocation());
                var options = createOptions(request, spans);
                var content = content(request.getContent());
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                Mono<MutationResult> mr;
                if (options == null) mr = collection.replace(docId, content);
                else mr = collection.replace(docId, content, options);
                return mr.map(r -> {
                    result.setElapsedNanos(System.nanoTime() - start);
                    if (op.getReturnResult()) populateResult(result, r);
                    else setSuccess(result);
                    return result.build();
                });
            } else if (op.hasUpsert()) {
                var request = op.getUpsert();
                var collection = connection.collection(request.getLocation()).reactive();
                var docId = getDocId(request.getLocation());
                var options = createOptions(request, spans);
                var content = content(request.getContent());
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                Mono<MutationResult> mr;
                if (options == null) mr = collection.upsert(docId, content);
                else mr = collection.upsert(docId, content, options);
                result.setElapsedNanos(System.nanoTime() - start);
                return mr.map(r -> {
                    if (op.getReturnResult()) populateResult(result, r);
                    else setSuccess(result);
                    return result.build();
                });
            // [if:3.4.1]
            } else if (op.hasRangeScan()) {
                var request = op.getRangeScan();
                var collection = connection.collection(request.getCollection()).reactive();
                var options = createOptions(request, spans);
                var scanType = convertScanType(request);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                Flux<ScanResult> results;
                if (options != null) results = collection.scan(scanType, options);
                else results = collection.scan(scanType);
                result.setElapsedNanos(System.nanoTime() - start);
                var streamer = new FluxStreamer<ScanResult>(results, perRun, request.getStreamConfig().getStreamId(), request.getStreamConfig(),
                        (ScanResult r) -> processScanResult(request, r),
                        (Throwable err) -> convertException(err));
                perRun.streamerOwner().addAndStart(streamer);
                result.setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                        .setCreated(com.couchbase.client.protocol.streams.Created.newBuilder()
                                .setType(STREAM_KV_RANGE_SCAN)
                                .setStreamId(streamer.streamId())));
                return Mono.just(result.build());
                // [end]
            } else if (op.hasClusterCommand()) {
              return handleClusterLevelCommand(op, perRun, result);
            } else if (op.hasBucketCommand()) {
              return handleBucketLevelCommand(op, perRun, result);
            } else if (op.hasScopeCommand()) {
              return handleScopeLevelCommand(op, perRun, result);
            } else if (op.hasCollectionCommand()) {
              return handleCollectionLevelCommand(op, perRun, result);
            } else {
              return Mono.error(new UnsupportedOperationException(new IllegalArgumentException("Unknown operation")));
            }
        });
    }

  private Mono<Result> handleCollectionLevelCommand(Command op, PerRun perRun, Result.Builder result) {
    var clc = op.getCollectionCommand();

    ReactiveCollection collection = null;
    if (clc.hasCollection()) {
      collection = connection.cluster()
              .bucket(clc.getCollection().getBucketName())
              .scope(clc.getCollection().getScopeName())
              .collection(clc.getCollection().getCollectionName())
              .reactive();
    }

    // [if:3.4.3]
    if (clc.hasQueryIndexManager()) {
      return QueryIndexManagerHelper.handleCollectionQueryIndexManagerReactive(collection, spans, op, result);
    }
    // [end]

    if (clc.hasGetAndLock()) {
      var request = clc.getGetAndLock();
      var docId = getDocId(request.getLocation());
      var duration = request.getDuration();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      Mono<GetResult> gr;
      if (options == null) {
        gr = collection.getAndLock(docId, Duration.ofSeconds(duration.getSeconds()));
      } else {
        gr = collection.getAndLock(docId, Duration.ofSeconds(duration.getSeconds()), options);
      }
      return gr.map(r -> {
        result.setElapsedNanos(System.nanoTime() - start);
        if (op.getReturnResult()) {
          populateResult(request.getContentAs(), result, r);
        } else {
          setSuccess(result);
        }
        return result.build();
      });
    }

    if (clc.hasUnlock()) {
      var request = clc.getUnlock();
      var docId = getDocId(request.getLocation());
      var cas = request.getCas();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      Mono<Void> gr;
      if (options == null) {
        gr = collection.unlock(docId, cas);
      } else {
        gr = collection.unlock(docId, cas, options);
      }
      return gr.then(Mono.fromCallable(() -> {
        result.setElapsedNanos(System.nanoTime() - start);
        setSuccess(result);
        return result.build();
      }));
    }

    if (clc.hasGetAndTouch()) {
      var request = clc.getGetAndTouch();
      var docId = getDocId(request.getLocation());
      Duration expiry;
      if (request.getExpiry().hasAbsoluteEpochSecs()) {
        expiry = Duration.between(Instant.now(), Instant.ofEpochSecond(request.getExpiry().getAbsoluteEpochSecs()));
      } else {
        expiry = Duration.ofSeconds(request.getExpiry().getRelativeSecs());
      }
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      Mono<GetResult> gr;
      if (options == null) {
        gr = collection.getAndTouch(docId, expiry);
      } else {
        gr = collection.getAndTouch(docId, expiry, options);
      }
      return gr.map(r -> {
        result.setElapsedNanos(System.nanoTime() - start);
        if (op.getReturnResult()) {
          populateResult(request.getContentAs(), result, r);
        } else {
          setSuccess(result);
        }
        return result.build();
      });
    }

    if (clc.hasTouch()) {
      var request = clc.getTouch();
      var docId = getDocId(request.getLocation());
      var options = createOptions(request, spans);
      Duration expiry;
      if (request.getExpiry().hasAbsoluteEpochSecs()) {
        expiry = Duration.between(Instant.now(), Instant.ofEpochSecond(request.getExpiry().getAbsoluteEpochSecs()));
      } else {
        expiry = Duration.ofSeconds(request.getExpiry().getRelativeSecs());
      }
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      Mono<MutationResult> mr;
      if (options == null) {
        mr = collection.touch(docId, expiry);
      } else {
        mr = collection.touch(docId, expiry, options);
      }
      return mr.map(r -> {
        result.setElapsedNanos(System.nanoTime() - start);
        if (op.getReturnResult()) {
          populateResult(result, r);
        } else {
          setSuccess(result);
        }
        return result.build();
      });
    }

    if (clc.hasExists()) {
      var request = clc.getExists();
      var docId = getDocId(request.getLocation());
      var exists = collection.exists(docId);
      return exists.map(r -> {
        if (op.getReturnResult()) {
          populateResult(result, r);
        } else {
          setSuccess(result);
        }
        return result.build();
      });
    }

    if (clc.hasMutateIn()) {
      var request = clc.getMutateIn();
      var docId = getDocId(request.getLocation());
      var options = createOptions(request);
      Mono<MutateInResult> mr;
      if (options == null) {
        mr = collection.mutateIn(docId, request.getSpecList().stream().map(v -> convertMutateInSpec(v)).toList());
      } else {
        mr = collection.mutateIn(docId, request.getSpecList().stream().map(v -> convertMutateInSpec(v)).toList(), options);
      }
      return mr.map(r -> {
        if (op.getReturnResult()) {
          populateResult(result, r, request);
        } else {
          setSuccess(result);
        }
        return result.build();
      });
    }

    if (clc.hasGetAllReplicas()) {
      var request = clc.getGetAllReplicas();
      var docId = getDocId(request.getLocation());
      var options = createOptions(request);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      Flux<GetReplicaResult> results;
      if (options == null) {
        results = collection.getAllReplicas(docId);
      } else {
        results = collection.getAllReplicas(docId, options);
      }
      result.setElapsedNanos(System.nanoTime() - start);
      var streamer = new FluxStreamer<>(results, perRun, request.getStreamConfig().getStreamId(), request.getStreamConfig(),
              (GetReplicaResult r) -> processGetAllReplicasResult(request, r),
              this::convertException);
      perRun.streamerOwner().addAndStart(streamer);
      result.setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
              .setCreated(com.couchbase.client.protocol.streams.Created.newBuilder()
                      .setType(STREAM_KV_GET_ALL_REPLICAS)
                      .setStreamId(streamer.streamId())));
      return Mono.just(result.build());
    }

    if (clc.hasGetAnyReplica()) {
      var request = clc.getGetAnyReplica();
      var docId = getDocId(request.getLocation());
      var options = createOptions(request);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      Mono<GetReplicaResult> gr;
      if (options == null) {
        gr = collection.getAnyReplica(docId);
      } else {
        gr = collection.getAnyReplica(docId, options);
      }
      return gr.map(r -> {
        result.setElapsedNanos(System.nanoTime() - start);
        if (op.getReturnResult()) {
          populateResult(result, r, request.getContentAs());
        } else {
          setSuccess(result);
        }
        return result.build();
      });
    }

    if (clc.hasBinary()) {

      var blc = clc.getBinary();

      if (blc.hasIncrement()) {
        var request = blc.getIncrement();
        var docId = getDocId(request.getLocation());
        var options = createOptions(request);
        result.setInitiated(getTimeNow());
        long start = System.nanoTime();
        Mono<CounterResult> cr;
        if (options == null) {
          cr = collection.binary().increment(docId);
        } else {
          cr = collection.binary().increment(docId, options);
        }
        result.setElapsedNanos(System.nanoTime() - start);
        return cr.map(r -> {
          if (op.getReturnResult()) {
            populateResult(result, r);
          } else {
            setSuccess(result);
          }
          return result.build();
        });
      }

      if (blc.hasDecrement()) {
        var request = blc.getDecrement();
        var docId = getDocId(request.getLocation());
        var options = createOptions(request);
        result.setInitiated(getTimeNow());
        long start = System.nanoTime();
        Mono<CounterResult> cr;
        if (options == null) {
          cr = collection.binary().decrement(docId);
        } else {
          cr = collection.binary().decrement(docId, options);
        }
        result.setElapsedNanos(System.nanoTime() - start);
        return cr.map(r -> {
          if (op.getReturnResult()) {
            populateResult(result, r);
          } else {
            setSuccess(result);
          }
          return result.build();
        });
      }

      if (blc.hasAppend()) {
        var request = blc.getAppend();
        var docId = getDocId(request.getLocation());
        var options = createOptions(request);
        result.setInitiated(getTimeNow());
        long start = System.nanoTime();
        Mono<MutationResult> mr;
        if (options == null) {
          mr = collection.binary().append(docId, request.getContent().toByteArray());
        } else {
          mr = collection.binary().append(docId, request.getContent().toByteArray(), options);
        }
        result.setElapsedNanos(System.nanoTime() - start);
        return mr.map(r -> {
          if (op.getReturnResult()) {
            populateResult(result, r);
          } else {
            setSuccess(result);
          }
          return result.build();
        });
      }

      if (blc.hasPrepend()) {
        var request = blc.getPrepend();
        var docId = getDocId(request.getLocation());
        var options = createOptions(request);
        result.setInitiated(getTimeNow());
        long start = System.nanoTime();
        Mono<MutationResult> mr;
        if (options == null) {
          mr = collection.binary().prepend(docId, request.getContent().toByteArray());
        } else {
          mr = collection.binary().prepend(docId, request.getContent().toByteArray(), options);
        }
        result.setElapsedNanos(System.nanoTime() - start);
        return mr.map(r -> {
          if (op.getReturnResult()) {
            populateResult(result, r);
          } else {
            setSuccess(result);
          }
          return result.build();
        });
      }
    }

    if (clc.hasLookupIn() || clc.hasLookupInAllReplicas() || clc.hasLookupInAnyReplica()) {
      return LookupInHelper.handleLookupInReactive(perRun, connection, op, this::getDocId, spans);
    }

    return Mono.error(new UnsupportedOperationException("Unknown command " + op));
  }

  private Mono<Result> handleScopeLevelCommand(Command op, PerRun perRun, Result.Builder result) {
    var slc = op.getScopeCommand();
    var scope = connection.cluster().bucket(slc.getScope().getBucketName()).scope(slc.getScope().getScopeName());

    // [if:3.0.9]
    if (slc.hasQuery()) {
      var request = slc.getQuery();
      String query = request.getStatement();

      result.setInitiated(getTimeNow());
      long start = System.nanoTime();

      Mono<ReactiveQueryResult> queryResult;
      if (request.hasOptions()) {
        queryResult = scope.reactive().query(query, createOptions(request, spans));
      } else {
        queryResult = scope.reactive().query(query);
      }

      return returnQueryResult(request, queryResult, result, start);
    }
    // [end]

    // [if:3.4.5]
    if (slc.hasSearch()) {
      return handleSearchQueryReactive(connection.cluster(), scope, spans, slc.getSearch(), perRun)
              .ofType(Result.class);
    } else if (slc.hasSearchIndexManager()) {
      return Mono.fromSupplier(() -> SearchHelper.handleScopeSearchIndexManager(scope, spans, op));
    }
    // [end]

    // [if:3.6.0]
    if (slc.hasSearchV2()) {
      // Streaming, so intentionally does not return a result.
      return handleSearchReactive(connection.cluster(), scope, spans, slc.getSearchV2(), perRun)
              .ofType(Result.class);
    }
    // [end]

    return Mono.error(new UnsupportedOperationException("Unknown command " + op));
  }

  private Mono<Result> handleBucketLevelCommand(Command op, PerRun perRun, Result.Builder result) {
    var blc = op.getBucketCommand();
    var bucket = connection.cluster().reactive().bucket(blc.getBucketName());

    if (blc.hasWaitUntilReady()) {
      var request = blc.getWaitUntilReady();

      logger.info("Calling waitUntilReady on bucket " + bucket + " with timeout " + request.getTimeoutMillis() + " milliseconds.");

      var timeout = Duration.ofMillis(request.getTimeoutMillis());

      Mono<Void> response;

      if (request.hasOptions()) {
        var options = waitUntilReadyOptions(request);
        response = bucket.waitUntilReady(timeout, options);
      } else {
        response = bucket.waitUntilReady(timeout);
      }

      return response.then(Mono.fromCallable(() -> {
        setSuccess(result);
        return result.build();
      }));
    }
    // [if:3.4.12]
    else if (blc.hasCollectionManager()) {
      return CollectionManagerHelper.handleCollectionManagerReactive(connection.cluster().reactive(), spans, op, result);
    }
    // [end]

    return Mono.error(new UnsupportedOperationException("Unknown command " + op));
  }

  private Mono<Result> handleClusterLevelCommand(Command op, PerRun perRun, Result.Builder result) {
    var clc = op.getClusterCommand();
    var cluster = connection.cluster().reactive();

    if (clc.hasWaitUntilReady()) {
      var request = clc.getWaitUntilReady();
      logger.info("Calling waitUntilReady with timeout " + request.getTimeoutMillis() + " milliseconds.");
      var timeout = Duration.ofMillis(request.getTimeoutMillis());

      Mono<Void> response;

      if (request.hasOptions()) {
        var options = waitUntilReadyOptions(request);
        response = cluster.waitUntilReady(timeout, options);

      } else {
        response = cluster.waitUntilReady(timeout);
      }

      return response.then(Mono.fromCallable(() -> {
        setSuccess(result);
        return result.build();
      }));

    }
    // [if:3.2.4]
    else if (clc.hasBucketManager()) {
      return BucketManagerHelper.handleBucketManagerReactive(cluster, spans, op, result);
    }
    // [end]
    // [if:3.2.1]
    else if (clc.hasEventingFunctionManager()) {
      return EventingHelper.handleEventingFunctionManagerReactive(cluster, spans, op, result);
    }
    // [end]

    // [if:3.4.3]
    if (clc.hasQueryIndexManager()) {
      return QueryIndexManagerHelper.handleClusterQueryIndexManagerReactive(cluster, spans, op, result);
    }
    // [end]

    // [if:3.4.5]
    if (clc.hasSearch()) {
      // Streaming, so intentionally does not return a result.
      return handleSearchQueryReactive(connection.cluster(), null, spans, clc.getSearch(), perRun)
              .ofType(Result.class);
    }

    if (clc.hasSearchIndexManager()) {
      // Skipping testing the reactive API as this is tertiary functionality, and the reactive API wraps the
      // same underlying logic as the blocking API.
      return Mono.fromSupplier(() -> SearchHelper.handleClusterSearchIndexManager(connection.cluster(), spans, op));
    }
    // [end]

    // [if:3.6.0]
    if (clc.hasSearchV2()) {
      // Streaming, so intentionally does not return a result.
      return handleSearchReactive(connection.cluster(), null, spans, clc.getSearchV2(), perRun)
              .ofType(Result.class);
    }
    // [end]

    if (clc.hasQuery()) {
      var request = clc.getQuery();
      String query = request.getStatement();

      result.setInitiated(getTimeNow());
      long start = System.nanoTime();

      Mono<ReactiveQueryResult> queryResult;
      if (request.hasOptions()) {
        queryResult = connection.cluster().reactive().query(query, createOptions(request, spans));
      } else {
        queryResult = connection.cluster().reactive().query(query);
      }

      return returnQueryResult(request, queryResult, result, start);
    }

    return Mono.error(new UnsupportedOperationException("Unknown command " + op));
  }

  @Override
    protected Exception convertException(Throwable raw) {
        return convertExceptionShared(raw);
    }

    private Mono<Result> returnQueryResult(com.couchbase.client.protocol.sdk.query.Command request, Mono<ReactiveQueryResult> queryResult, Result.Builder result, Long start) {
        return queryResult.publishOn(Schedulers.boundedElastic()).map(r -> {
            result.setElapsedNanos(System.nanoTime() - start);

            var builder = com.couchbase.client.protocol.sdk.query.QueryResult.newBuilder();

            // FIT only supports testing blocking (not streaming) queries currently, so the .block() here to gather
            // the rows is fine.
            var content = ContentAsUtil.contentTypeList(request.getContentAs(),
                    () -> r.rowsAs(byte[].class).collectList().block(),
                    () -> r.rowsAs(String.class).collectList().block(),
                    () -> r.rowsAs(JsonObject.class).collectList().block(),
                    () -> r.rowsAs(JsonArray.class).collectList().block(),
                    () -> r.rowsAs(Boolean.class).collectList().block(),
                    () -> r.rowsAs(Integer.class).collectList().block(),
                    () -> r.rowsAs(Double.class).collectList().block());

            if (content.isFailure()) {
              throw content.exception();
            }

            builder.addAllContent(content.value());

            // Metadata
            var convertedMetaData = convertMetaData(r.metaData().block());
            builder.setMetaData(convertedMetaData);

            result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                    .setQueryResult(builder));

            return result.build();
        });
    }
}

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
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.DefaultJsonSerializer;
import com.couchbase.client.java.codec.JsonTranscoder;
import com.couchbase.client.java.codec.LegacyTranscoder;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.codec.RawStringTranscoder;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.diagnostics.WaitUntilReadyOptions;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.query.*;
import com.couchbase.client.protocol.sdk.Command;
import com.couchbase.client.protocol.sdk.collection.mutatein.MutateIn;
import com.couchbase.client.protocol.sdk.collection.mutatein.MutateInMacro;
import com.couchbase.client.protocol.sdk.collection.mutatein.MutateInSpecResult;
import com.couchbase.client.protocol.sdk.kv.GetAllReplicas;
import com.couchbase.client.protocol.shared.*;
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
import com.couchbase.client.java.kv.ScanOptions;
import com.couchbase.client.java.kv.ScanResult;
// [end]
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.performer.core.commands.SdkCommandExecutor;
import com.couchbase.client.performer.core.perf.Counters;
import com.couchbase.client.performer.core.perf.PerRun;
import com.couchbase.client.performer.core.stream.StreamStreamer;
import com.couchbase.client.performer.core.util.ErrorUtil;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.cluster.waituntilready.WaitUntilReadyRequest;
import com.couchbase.client.protocol.sdk.kv.rangescan.Scan;
// [if:3.4.3]
import com.couchbase.query.QueryIndexManagerHelper;
// [end]
import com.couchbase.client.protocol.shared.Exception;
import com.couchbase.utils.ClusterConnection;
import com.couchbase.utils.ContentAsUtil;
import com.google.protobuf.ByteString;
// [if:3.4.5]
import com.couchbase.search.SearchHelper;
import static com.couchbase.search.SearchHelper.handleSearchQueryBlocking;
// [end]
// [if:3.6.0]
import static com.couchbase.search.SearchHelper.handleSearchBlocking;
// [end]

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.couchbase.client.performer.core.util.TimeUtil.getTimeNow;
import static com.couchbase.client.protocol.streams.Type.STREAM_KV_RANGE_SCAN;
import static com.couchbase.client.protocol.streams.Type.STREAM_KV_GET_ALL_REPLICAS;


/**
 * SdkOperation performs each requested SDK operation
 */
public class JavaSdkCommandExecutor extends SdkCommandExecutor {
    private static final JsonTranscoder JSON_TRANSCODER = JsonTranscoder.create(DefaultJsonSerializer.create());
    private static final LegacyTranscoder LEGACY_TRANSCODER = LegacyTranscoder.create(DefaultJsonSerializer.create());

    private final ClusterConnection connection;
    private final ConcurrentHashMap<String, RequestSpan> spans;

    public JavaSdkCommandExecutor(ClusterConnection connection, Counters counters, ConcurrentHashMap<String, RequestSpan> spans) {
        super(counters);
        this.connection = connection;
        this.spans = spans;
    }

    @Override
    protected void performOperation(com.couchbase.client.protocol.sdk.Command op, PerRun perRun) {
        var result = performOperationInternal(op, perRun);
        perRun.resultsStream().enqueue(result);
    }

    protected com.couchbase.client.protocol.run.Result performOperationInternal(com.couchbase.client.protocol.sdk.Command op, PerRun perRun) {
        var result = com.couchbase.client.protocol.run.Result.newBuilder();

        if (op.hasInsert()){
            var request = op.getInsert();
            var collection = connection.collection(request.getLocation());
            var content = content(request.getContent());
            var docId = getDocId(request.getLocation());
            var options = createOptions(request, spans);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            MutationResult mr;
            if (options == null) mr = collection.insert(docId, content);
            else mr = collection.insert(docId, content, options);
            result.setElapsedNanos(System.nanoTime() - start);
            if (op.getReturnResult()) populateResult(result, mr);
            else setSuccess(result);
        } else if (op.hasGet()) {
            var request = op.getGet();
            var collection = connection.collection(request.getLocation());
            var docId = getDocId(request.getLocation());
            var options = createOptions(request, spans);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            GetResult gr;
            if (options == null) gr = collection.get(docId);
            else gr = collection.get(docId, options);
            result.setElapsedNanos(System.nanoTime() - start);
            if (op.getReturnResult()) populateResult(request.getContentAs(), result, gr);
            else setSuccess(result);
        } else if (op.hasRemove()){
            var request = op.getRemove();
            var collection = connection.collection(request.getLocation());
            var docId = getDocId(request.getLocation());
            var options = createOptions(request, spans);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            MutationResult mr;
            if (options == null) mr = collection.remove(docId);
            else mr = collection.remove(docId, options);
            result.setElapsedNanos(System.nanoTime() - start);
            if (op.getReturnResult()) populateResult(result, mr);
            else setSuccess(result);
        } else if (op.hasReplace()){
            var request = op.getReplace();
            var collection = connection.collection(request.getLocation());
            var docId = getDocId(request.getLocation());
            var options = createOptions(request, spans);
            var content = content(request.getContent());
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            MutationResult mr;
            if (options == null) mr = collection.replace(docId, content);
            else mr = collection.replace(docId, content, options);
            result.setElapsedNanos(System.nanoTime() - start);
            if (op.getReturnResult()) populateResult(result, mr);
            else setSuccess(result);
        } else if (op.hasUpsert()){
            var request = op.getUpsert();
            var collection = connection.collection(request.getLocation());
            var docId = getDocId(request.getLocation());
            var options = createOptions(request, spans);
            var content = content(request.getContent());
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            MutationResult mr;
            if (options == null) mr = collection.upsert(docId, content);
            else mr = collection.upsert(docId, content, options);
            result.setElapsedNanos(System.nanoTime() - start);
            if (op.getReturnResult()) populateResult(result, mr);
            else setSuccess(result);
        // [if:3.4.1]
        } else if (op.hasRangeScan()){
            var request = op.getRangeScan();
            var collection = connection.collection(request.getCollection());
            var options = createOptions(request, spans);
            var scanType = convertScanType(request);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            Stream<ScanResult> results;
            if (options != null) results = collection.scan(scanType, options);
            else results = collection.scan(scanType);
            result.setElapsedNanos(System.nanoTime() - start);
            var streamer = new StreamStreamer<ScanResult>(results, perRun, request.getStreamConfig().getStreamId(), request.getStreamConfig(),
                    (ScanResult r) -> processScanResult(request, r),
                    (Throwable err) -> convertException(err));
            perRun.streamerOwner().addAndStart(streamer);
            result.setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                    .setCreated(com.couchbase.client.protocol.streams.Created.newBuilder()
                            .setType(STREAM_KV_RANGE_SCAN)
                            .setStreamId(streamer.streamId())));
        // [end]
        } else if (op.hasClusterCommand()) {
            return handleClusterLevelCommand(op, result);
        } else if (op.hasBucketCommand()) {
            return handleBucketLevelCommand(op, result);
        } else if (op.hasScopeCommand()) {
            return handleScopeLevelCommand(op, result);
        } else if (op.hasCollectionCommand()) {
            return handleCollectionLevelCommand(op, perRun, result);
        } else {
            throw new UnsupportedOperationException(new IllegalArgumentException("Unknown operation"));
        }

        return result.build();
    }

  private Result handleCollectionLevelCommand(Command op, PerRun perRun, Result.Builder result) {
    var clc = op.getCollectionCommand();

    Collection collection = null;
    if (clc.hasCollection()) {
      collection = connection.cluster()
              .bucket(clc.getCollection().getBucketName())
              .scope(clc.getCollection().getScopeName())
              .collection(clc.getCollection().getCollectionName());
    }

    // [if:3.4.3]
    if (clc.hasQueryIndexManager()) {
      QueryIndexManagerHelper.handleCollectionQueryIndexManager(collection, spans, op, result);
    }
    // [end]

    if (clc.hasGetAndLock()) {
      var request = clc.getGetAndLock();
      var docId = getDocId(request.getLocation());
      var duration = Duration.ofSeconds(clc.getGetAndLock().getDuration().getSeconds());
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      GetResult gr;
      if (options == null) gr = collection.getAndLock(docId, duration);
      else gr = collection.getAndLock(docId, duration, options);
      result.setElapsedNanos(System.nanoTime() - start);
      if (op.getReturnResult()) populateResult(request.getContentAs(), result, gr);
      else setSuccess(result);
    }

    if (clc.hasUnlock()) {
      var request = clc.getUnlock();
      var docId = getDocId(request.getLocation());
      var cas = request.getCas();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (options == null) collection.unlock(docId, cas);
      else collection.unlock(docId, cas, options);
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    }

    if (clc.hasGetAndTouch()) {
      var request = clc.getGetAndTouch();
      var docId = getDocId(request.getLocation());
      Duration expiry;
      if (request.getExpiry().hasAbsoluteEpochSecs()) {
        expiry = Duration.between(Instant.now(), Instant.ofEpochSecond(request.getExpiry().getAbsoluteEpochSecs()));
      } else expiry = Duration.ofSeconds(request.getExpiry().getRelativeSecs());
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      GetResult gr;
      if (options == null) gr = collection.getAndTouch(docId, expiry);
      else gr = collection.getAndTouch(docId, expiry, options);
      result.setElapsedNanos(System.nanoTime() - start);
      if (op.getReturnResult()) populateResult(request.getContentAs(), result, gr);
      else setSuccess(result);
    }

    if (clc.hasTouch()) {
      var request = clc.getTouch();
      var docId = getDocId(request.getLocation());
      var options = createOptions(request, spans);
      Duration expiry;
      if (request.getExpiry().hasAbsoluteEpochSecs()) {
        expiry = Duration.between(Instant.now(), Instant.ofEpochSecond(request.getExpiry().getAbsoluteEpochSecs()));
      } else expiry = Duration.ofSeconds(request.getExpiry().getRelativeSecs());
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      MutationResult mr;
      if (options == null) mr = collection.touch(docId, expiry);
      else mr = collection.touch(docId, expiry, options);
      result.setElapsedNanos(System.nanoTime() - start);
      if (op.getReturnResult()) populateResult(result, mr);
      else setSuccess(result);
    }

    if (clc.hasExists()) {
      var request = clc.getExists();
      var docId = getDocId(request.getLocation());
      var options = createOptions(request);
      ExistsResult exists;
      if (options == null) exists = collection.exists(docId);
      else exists = collection.exists(docId, options);
      if (op.getReturnResult()) populateResult(result, exists);
      else setSuccess(result);
    }

    if (clc.hasMutateIn()) {
      var request = clc.getMutateIn();
      var docId = getDocId(request.getLocation());
      var options = createOptions(request);
      var requestList = request.getSpecList().stream().map(JavaSdkCommandExecutor::convertMutateInSpec).toList();
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      MutateInResult mr;
      if (options == null) mr = collection.mutateIn(docId, requestList);
      else mr = collection.mutateIn(docId, requestList, options);
      result.setElapsedNanos(System.nanoTime() - start);
      if (op.getReturnResult()) populateResult(result, mr, request);
      else setSuccess(result);
    }

    if (clc.hasBinary()) {
      var blc = clc.getBinary();

      if (blc.hasIncrement()) {
        var request = blc.getIncrement();
        var docId = getDocId(request.getLocation());
        var options = createOptions(request);
        result.setInitiated(getTimeNow());
        long start = System.nanoTime();
        CounterResult cr;
        if (options == null) cr = collection.binary().increment(docId);
        else cr = collection.binary().increment(docId, options);
        result.setElapsedNanos(System.nanoTime() - start);
        if (op.getReturnResult()) populateResult(result, cr);
        else setSuccess(result);
      }

      if (blc.hasDecrement()) {
        var request = blc.getDecrement();
        var docId = getDocId(request.getLocation());
        var options = createOptions(request);
        result.setInitiated(getTimeNow());
        long start = System.nanoTime();
        CounterResult cr;
        if (options == null) cr = collection.binary().decrement(docId);
        else cr = collection.binary().decrement(docId, options);
        result.setElapsedNanos(System.nanoTime() - start);
        if (op.getReturnResult()) populateResult(result, cr);
        else setSuccess(result);
      }

      if (blc.hasAppend()) {
        var request = blc.getAppend();
        var docId = getDocId(request.getLocation());
        var options = createOptions(request);
        result.setInitiated(getTimeNow());
        long start = System.nanoTime();
        MutationResult mr;
        if (options == null) mr = collection.binary().append(docId, request.getContent().toByteArray());
        else mr = collection.binary().append(docId, request.getContent().toByteArray(), options);
        result.setElapsedNanos(System.nanoTime() - start);
        if (op.getReturnResult()) populateResult(result, mr);
        else setSuccess(result);
      }

      if (blc.hasPrepend()) {
        var request = blc.getPrepend();
        var docId = getDocId(request.getLocation());
        var options = createOptions(request);
        result.setInitiated(getTimeNow());
        long start = System.nanoTime();
        MutationResult mr;
        if (options == null) mr = collection.binary().prepend(docId, request.getContent().toByteArray());
        else mr = collection.binary().prepend(docId, request.getContent().toByteArray(), options);
        result.setElapsedNanos(System.nanoTime() - start);
        if (op.getReturnResult()) populateResult(result, mr);
        else setSuccess(result);
      }
    }

    if (clc.hasGetAllReplicas()) {
      var request = clc.getGetAllReplicas();
      var docId = getDocId(request.getLocation());
      var options = createOptions(request);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      Stream<GetReplicaResult> results;
      if (options == null) results = collection.getAllReplicas(docId);
      else results = collection.getAllReplicas(docId, options);
      result.setElapsedNanos(System.nanoTime() - start);
      var streamer = new StreamStreamer<GetReplicaResult>(results, perRun, request.getStreamConfig().getStreamId(), request.getStreamConfig(),
              (GetReplicaResult r) -> processGetAllReplicasResult(request, r),
              (Throwable err) -> convertException(err));
      perRun.streamerOwner().addAndStart(streamer);
      result.setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
              .setCreated(com.couchbase.client.protocol.streams.Created.newBuilder()
                      .setType(STREAM_KV_GET_ALL_REPLICAS)
                      .setStreamId(streamer.streamId())));
    }

    if (clc.hasGetAnyReplica()) {
      var request = clc.getGetAnyReplica();
      var docId = getDocId(request.getLocation());
      var options = createOptions(request);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      GetReplicaResult mr;
      if (options == null) mr = collection.getAnyReplica(docId);
      else mr = collection.getAnyReplica(docId, options);
      result.setElapsedNanos(System.nanoTime() - start);
      if (op.getReturnResult()) populateResult(result, mr, request.getContentAs());
      else setSuccess(result);
    }

    if (clc.hasLookupIn() || clc.hasLookupInAllReplicas() || clc.hasLookupInAnyReplica()) {
      result = LookupInHelper.handleLookupIn(perRun, connection, op, this::getDocId, spans);
    }
    return result.build();
  }

  private Result handleScopeLevelCommand(Command op, Result.Builder result) {
    var slc = op.getScopeCommand();
    var scope = connection.cluster().bucket(slc.getScope().getBucketName()).scope(slc.getScope().getScopeName());

    // [if:3.0.9]
    if (slc.hasQuery()) {
      var query = slc.getQuery().getStatement();
      QueryResult qr;
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (slc.getQuery().hasOptions()) qr = scope.query(query, createOptions(slc.getQuery(), spans));
      else qr = scope.query(query);
      result.setElapsedNanos(System.nanoTime() - start);
      if (op.getReturnResult()) populateResult(slc.getQuery(), result, qr);
      else setSuccess(result);
      return result.build();
    }
    // [end]

    // [if:3.4.5]
    if (slc.hasSearch()) {
      com.couchbase.client.protocol.sdk.search.Search command = slc.getSearch();
      return handleSearchQueryBlocking(connection.cluster(), scope, spans, command, op);
    } else if (slc.hasSearchIndexManager()) {
      return SearchHelper.handleScopeSearchIndexManager(scope, spans, op);
    }
    // [end]

    // [if:3.6.0]
    if (slc.hasSearchV2()) {
      return handleSearchBlocking(connection.cluster(), scope, spans, slc.getSearchV2(), op);
    }
    // [end]

    throw new UnsupportedOperationException("Unknown scope-level command");
  }

  private Result handleBucketLevelCommand(Command op, Result.Builder result) {
    var blc = op.getBucketCommand();
    var bucket = connection.cluster().bucket(blc.getBucketName());

    if (blc.hasWaitUntilReady()) {
      var request = blc.getWaitUntilReady();

      logger.info("Calling waitUntilReady on bucket " + bucket + " with timeout " + request.getTimeoutMillis() + " milliseconds.");

      var timeout = Duration.ofMillis(request.getTimeoutMillis());

      if (request.hasOptions()) {
        var options = waitUntilReadyOptions(request);
        bucket.waitUntilReady(timeout, options);

      } else {
        bucket.waitUntilReady(timeout);
      }

      setSuccess(result);
    }

    // [if:3.4.12]
    else if (blc.hasCollectionManager()) {
      CollectionManagerHelper.handleCollectionManager(connection.cluster(), spans, op, result);
    }
    // [end]

    return result.build();
  }

  private Result handleClusterLevelCommand(Command op, Result.Builder result) {
    var clc = op.getClusterCommand();

    if (clc.hasWaitUntilReady()) {
      var request = clc.getWaitUntilReady();
      logger.info("Calling waitUntilReady with timeout " + request.getTimeoutMillis() + " milliseconds.");
      var timeout = Duration.ofMillis(request.getTimeoutMillis());

      if (request.hasOptions()) {
        var options = waitUntilReadyOptions(request);
        connection.cluster().waitUntilReady(timeout, options);

      } else {
        connection.cluster().waitUntilReady(timeout);
      }

      setSuccess(result);

    }
    // [if:3.2.4]
    else if (clc.hasBucketManager()) {
      BucketManagerHelper.handleBucketManger(connection.cluster(), spans, op, result);
    }
    // [end]
    // [if:3.2.1]
    else if (clc.hasEventingFunctionManager()) {
      EventingHelper.handleEventingFunctionManager(connection.cluster(), spans, op, result);
    }
    // [end]

    // [if:3.4.3]
    if (clc.hasQueryIndexManager()) {
      QueryIndexManagerHelper.handleClusterQueryIndexManager(connection.cluster(), spans, op, result);
    }
    // [end]

    // [if:3.4.5]
    if (clc.hasSearch()) {
      com.couchbase.client.protocol.sdk.search.Search command = clc.getSearch();
      return handleSearchQueryBlocking(connection.cluster(), null, spans, command, op);
    } else if (clc.hasSearchIndexManager()) {
      return SearchHelper.handleClusterSearchIndexManager(connection.cluster(), spans, op);
    }
    // [end]

    if (clc.hasQuery()) {
      var query = clc.getQuery().getStatement();
      QueryResult qr;
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (clc.getQuery().hasOptions()) {
        qr = connection.cluster().query(query, createOptions(clc.getQuery(), spans));
      } else {
        qr = connection.cluster().query(query);
      }
      result.setElapsedNanos(System.nanoTime() - start);
      if (op.getReturnResult()) populateResult(clc.getQuery(), result, qr);
      else setSuccess(result);
    }

    // [if:3.6.0]
    if (clc.hasSearchV2()) {
      return handleSearchBlocking(connection.cluster(), null, spans, clc.getSearchV2(), op);
    }
    // [end]

    return result.build();
  }

  public static MutateInSpec convertMutateInSpec(com.couchbase.client.protocol.sdk.collection.mutatein.MutateInSpec requestSpec) {
        if (requestSpec.hasUpsert()) {
            var spec = MutateInSpec.upsert(
                    requestSpec.getUpsert().getPath(),
                    contentOrMacro(requestSpec.getUpsert().getContent()));
            if (requestSpec.getUpsert().hasXattr()) spec.xattr();
            if (requestSpec.getUpsert().hasCreatePath()) spec.createPath();
            return spec;
        }
        if (requestSpec.hasInsert()) {
            var spec = MutateInSpec.insert(
                    requestSpec.getInsert().getPath(),
                    contentOrMacro(requestSpec.getInsert().getContent()));
            if (requestSpec.getInsert().hasXattr()) spec.xattr();
            if (requestSpec.getInsert().hasCreatePath()) spec.createPath();
            return spec;
        }
        if (requestSpec.hasReplace()) {
            var spec = MutateInSpec.replace(
                    requestSpec.getReplace().getPath(),
                    contentOrMacro(requestSpec.getReplace().getContent()));
            if (requestSpec.getReplace().hasXattr()) spec.xattr();
            return spec;
        }
        if (requestSpec.hasRemove())
        {
            var spec = MutateInSpec.remove(requestSpec.getRemove().getPath());
            if (requestSpec.getRemove().hasXattr()) spec.xattr();
            return spec;
        }
        if (requestSpec.hasArrayAppend()) {
            var spec  = MutateInSpec.arrayAppend(
                    requestSpec.getArrayAppend().getPath(),
                    requestSpec.getArrayAppend().getContentList()
                            .stream()
                            .map(JavaSdkCommandExecutor::contentOrMacro)
                            .toList()
            );
            if (requestSpec.getArrayAppend().hasXattr()) spec.xattr();
            if (requestSpec.getArrayAppend().hasCreatePath()) spec.createPath();
            return spec;
        }
        if (requestSpec.hasArrayPrepend()) {
            var spec = MutateInSpec.arrayPrepend(
                    requestSpec.getArrayPrepend().getPath(),
                    requestSpec.getArrayPrepend().getContentList()
                            .stream()
                            .map(JavaSdkCommandExecutor::contentOrMacro)
                            .toList()
            );
            if (requestSpec.getArrayPrepend().hasXattr()) spec.xattr();
            if (requestSpec.getArrayPrepend().hasCreatePath()) spec.createPath();
            return spec;
        }
        if (requestSpec.hasArrayInsert()) {
            var spec = MutateInSpec.arrayInsert(
                    requestSpec.getArrayInsert().getPath(),
                    requestSpec.getArrayInsert().getContentList()
                            .stream()
                            .map(JavaSdkCommandExecutor::contentOrMacro)
                            .toList()
            );
            if (requestSpec.getArrayInsert().hasXattr()) spec.xattr();
            if (requestSpec.getArrayInsert().hasCreatePath()) spec.createPath();
            return spec;
        }
        if (requestSpec.hasArrayAddUnique()) {
            var spec = MutateInSpec.arrayAddUnique(
                    requestSpec.getArrayAddUnique().getPath(),
                    contentOrMacro(requestSpec.getArrayAddUnique().getContent()));
            if (requestSpec.getArrayAddUnique().hasXattr()) spec.xattr();
            if (requestSpec.getArrayAddUnique().hasCreatePath()) spec.createPath();
            return spec;
        }
        if (requestSpec.hasIncrement()) {
            var spec = MutateInSpec.increment(
                    requestSpec.getIncrement().getPath(),
                    requestSpec.getIncrement().getDelta());
            if (requestSpec.getIncrement().hasXattr()) spec.xattr();
            if (requestSpec.getIncrement().hasCreatePath()) spec.createPath();
            return spec;
        }
        if (requestSpec.hasDecrement()) {
            var spec = MutateInSpec.decrement(
                    requestSpec.getDecrement().getPath(),
                    requestSpec.getDecrement().getDelta());
            if (requestSpec.getDecrement().hasXattr()) spec.xattr();
            if (requestSpec.getDecrement().hasCreatePath()) spec.createPath();
            return spec;
        }
        throw new UnsupportedOperationException("Given MutateInSpec operation is unsupported");
    }

    // [if:3.4.1]
    public static Result processScanResult(Scan request, ScanResult r) {
        try {
            var builder = com.couchbase.client.protocol.sdk.kv.rangescan.ScanResult.newBuilder()
                    .setId(r.id())
                    .setIdOnly(r.idOnly())
                    .setStreamId(request.getStreamConfig().getStreamId());

            if (!r.idOnly()) {
                builder.setCas(r.cas());
                if (r.expiryTime().isPresent()) {
                    builder.setExpiryTime(r.expiryTime().get().getEpochSecond());
                }
            }

            if (request.hasContentAs()) {
              var content = ContentAsUtil.contentType(request.getContentAs(),
                      () -> r.contentAs(byte[].class),
                      () -> r.contentAs(String.class),
                      () -> r.contentAs(JsonObject.class),
                      () -> r.contentAs(JsonArray.class),
                      () -> r.contentAs(Boolean.class),
                      () -> r.contentAs(Integer.class),
                      () -> r.contentAs(Double.class));

              if (content.isFailure()) {
                throw content.exception();
              }

              builder.setContent(content.value());
            }

            return Result.newBuilder()
                    .setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                            .setRangeScanResult(builder.build()))
                    .build();
        } catch (RuntimeException err) {
            return Result.newBuilder()
                    .setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                            .setError(com.couchbase.client.protocol.streams.Error.newBuilder()
                                    .setException(convertExceptionShared(err))
                                    .setStreamId(request.getStreamConfig().getStreamId())))
                    .build();
        }
    }

    public static Optional<com.couchbase.client.java.kv.ScanTerm> convertScanTerm(com.couchbase.client.protocol.sdk.kv.rangescan.ScanTermChoice st) {
        if (st.hasDefault()) {
            return Optional.empty();
        }
        else if (st.hasMaximum()) {
            return Optional.empty();
        }
        else if (st.hasMinimum()) {
            return Optional.empty();
        }
        else if (st.hasTerm()) {
            var stt = st.getTerm();
            if (stt.hasExclusive() && stt.getExclusive()) {
                if (stt.hasAsString()) {
                    return Optional.of(com.couchbase.client.java.kv.ScanTerm.exclusive(stt.getAsString()));
                }
                else throw new UnsupportedOperationException();
            }
            if (stt.hasAsString()) {
                return Optional.of(com.couchbase.client.java.kv.ScanTerm.inclusive(stt.getAsString()));
            }
            else throw new UnsupportedOperationException();
        }
        else throw new UnsupportedOperationException();
    }

    public static com.couchbase.client.java.kv.ScanType convertScanType(com.couchbase.client.protocol.sdk.kv.rangescan.Scan request) {
        if (request.getScanType().hasRange()) {
            var rs = request.getScanType().getRange();
            if (rs.hasFromTo()) {
                var from = convertScanTerm(rs.getFromTo().getFrom());
                var to = convertScanTerm(rs.getFromTo().getTo());
                if (from.isPresent() && to.isPresent()) {
                    return com.couchbase.client.java.kv.ScanType.rangeScan(from.get(), to.get());
                }
                else if (from.isPresent()) {
                    return com.couchbase.client.java.kv.ScanType.rangeScan(from.get(), null);
                }
                else if (to.isPresent()) {
                    return com.couchbase.client.java.kv.ScanType.rangeScan(null, to.get());
                }
                else {
                    return com.couchbase.client.java.kv.ScanType.rangeScan(null, null);
                }
            }
            else if (rs.hasDocIdPrefix()) {
                return com.couchbase.client.java.kv.ScanType.prefixScan(rs.getDocIdPrefix());
            }
            else throw new UnsupportedOperationException();
        }
        else if (request.getScanType().hasSampling()) {
            var ss = request.getScanType().getSampling();
            if (ss.hasSeed()) {
                return com.couchbase.client.java.kv.ScanType.samplingScan(ss.getLimit(), ss.getSeed());
            }
            return com.couchbase.client.java.kv.ScanType.samplingScan(ss.getLimit());
        }
        else {
            throw new UnsupportedOperationException();
        }
    }
    // [end]

    @Override
    protected Exception convertException(Throwable raw) {
        return convertExceptionShared(raw);
    }

    public static Exception convertExceptionShared(Throwable raw) {
        var ret = com.couchbase.client.protocol.shared.Exception.newBuilder();

        if (raw instanceof CouchbaseException || raw instanceof UnsupportedOperationException) {
            CouchbaseExceptionType type;
            if (raw instanceof UnsupportedOperationException) {
                type = CouchbaseExceptionType.SDK_UNSUPPORTED_OPERATION_EXCEPTION;
            }
            else {
                var err = (CouchbaseException) raw;
                type = ErrorUtil.convertException(err);
            }

            if (type != null) {
                var out = CouchbaseExceptionEx.newBuilder()
                        .setName(raw.getClass().getSimpleName())
                        .setType(type)
                        .setSerialized(raw.toString());
                if (raw.getCause() != null) {
                    out.setCause(convertExceptionShared(raw.getCause()));
                }

                ret.setCouchbase(out);
            }
        }
        else {
            ret.setOther(ExceptionOther.newBuilder()
                    .setName(raw.getClass().getSimpleName())
                    .setSerialized(raw.toString()));
        }

        return ret.build();
    }

    public static void setSuccess(com.couchbase.client.protocol.run.Result.Builder result) {
        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setSuccess(true));
    }

    public static void populateResult(com.couchbase.client.protocol.run.Result.Builder result, MutationResult value) {
        var builder = com.couchbase.client.protocol.sdk.kv.MutationResult.newBuilder()
                .setCas(value.cas());
        value.mutationToken().ifPresent(mt ->
            builder.setMutationToken(com.couchbase.client.protocol.shared.MutationToken.newBuilder()
                            .setPartitionId(mt.partitionID())
                            .setPartitionUuid(mt.partitionUUID())
                            .setSequenceNumber(mt.sequenceNumber())
                            .setBucketName(mt.bucketName())));
        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setMutationResult(builder));
    }

  public static void populateResult(ContentAs contentAs, Result.Builder result, GetResult value) {
    var content = ContentAsUtil.contentType(contentAs,
            () -> value.contentAs(byte[].class),
            () -> value.contentAs(String.class),
            () -> value.contentAs(JsonObject.class),
            () -> value.contentAs(JsonArray.class),
            () -> value.contentAs(Boolean.class),
            () -> value.contentAs(Integer.class),
            () -> value.contentAs(Double.class));

    if (content.isSuccess()) {
      var builder = com.couchbase.client.protocol.sdk.kv.GetResult.newBuilder()
              .setCas(value.cas())
              .setContent(content.value());

        // [if:3.0.7]
        value.expiryTime().ifPresent(et -> builder.setExpiryTime(et.getEpochSecond()));
        // [end]

        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setGetResult(builder));
      }
      else {
        throw content.exception();
      }
    }

    public static void populateResult(com.couchbase.client.protocol.run.Result.Builder result, MutateInResult value, MutateIn request) {
        var builder = com.couchbase.client.protocol.sdk.collection.mutatein.MutateInResult.newBuilder()
                .setCas(value.cas());
        value.mutationToken().ifPresent(mt ->
                builder.setMutationToken(com.couchbase.client.protocol.shared.MutationToken.newBuilder()
                        .setPartitionId(mt.partitionID())
                        .setPartitionUuid(mt.partitionUUID())
                        .setSequenceNumber(mt.sequenceNumber())
                        .setBucketName(mt.bucketName())));

        AtomicInteger index = new AtomicInteger();
        request.getSpecList().forEach(spec -> {
            if (spec.hasContentAs()) {
                var content = ContentAsUtil.contentType(spec.getContentAs(),
                        () -> value.contentAs(index.get(), byte[].class),
                        () -> value.contentAs(index.get(), String.class),
                        () -> value.contentAs(index.get(), JsonObject.class),
                        () -> value.contentAs(index.get(), JsonArray.class),
                        () -> value.contentAs(index.get(), Boolean.class),
                        () -> value.contentAs(index.get(), Integer.class),
                        () -> value.contentAs(index.getAndIncrement(), Double.class));
                builder.addResults(
                        MutateInSpecResult.newBuilder()
                                .setContentAsResult(ContentOrError.newBuilder().setContent(content.value()).build())
                                .build()
                );
            }
            else builder.addResults(MutateInSpecResult.newBuilder().build());
        });

        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setMutateInResult(builder));
    }

    public static void populateResult(com.couchbase.client.protocol.run.Result.Builder result, CounterResult value) {
        var builder = com.couchbase.client.protocol.sdk.kv.CounterResult.newBuilder()
                .setCas(value.cas())
                .setContent(value.content());
        value.mutationToken().ifPresent(mt ->
                builder.setMutationToken(com.couchbase.client.protocol.shared.MutationToken.newBuilder()
                        .setPartitionId(mt.partitionID())
                        .setPartitionUuid(mt.partitionUUID())
                        .setSequenceNumber(mt.sequenceNumber())
                        .setBucketName(mt.bucketName())));
        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setCounterResult(builder));
    }

    public static Result processGetAllReplicasResult(GetAllReplicas request, GetReplicaResult value) {
        try {

            var content = ContentAsUtil.contentType(request.getContentAs(),
                    () -> value.contentAs(byte[].class),
                    () -> value.contentAs(String.class),
                    () -> value.contentAs(JsonObject.class),
                    () -> value.contentAs(JsonArray.class),
                    () -> value.contentAs(Boolean.class),
                    () -> value.contentAs(Integer.class),
                    () -> value.contentAs(Double.class));

            var builder = com.couchbase.client.protocol.sdk.kv.GetReplicaResult.newBuilder()
                    .setCas(value.cas())
                    .setContent(content.value())
                    .setIsReplica(value.isReplica())
                    .setStreamId(request.getStreamConfig().getStreamId());

            return Result.newBuilder()
                    .setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                            .setGetReplicaResult(builder.build()))
                    .build();
        } catch (RuntimeException err) {
            return Result.newBuilder()
                    .setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                            .setError(com.couchbase.client.protocol.streams.Error.newBuilder()
                                    .setException(convertExceptionShared(err))
                                    .setStreamId(request.getStreamConfig().getStreamId())))
                    .build();
        }
    }

    public static void populateResult(com.couchbase.client.protocol.run.Result.Builder result, GetReplicaResult value, ContentAs contentAs) {
        var content = ContentAsUtil.contentType(contentAs,
                () -> value.contentAs(byte[].class),
                () -> value.contentAs(String.class),
                () -> value.contentAs(JsonObject.class),
                () -> value.contentAs(JsonArray.class),
                () -> value.contentAs(Boolean.class),
                () -> value.contentAs(Integer.class),
                () -> value.contentAs(Double.class));

        var builder = com.couchbase.client.protocol.sdk.kv.GetReplicaResult.newBuilder()
                .setCas(value.cas())
                .setContent(content.value())
                .setIsReplica(value.isReplica());

        // [if:3.0.7]
        value.expiryTime().ifPresent(et -> builder.setExpiryTime(et.getEpochSecond()));
        // [end]

        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setGetReplicaResult(builder));
    }

    public static void populateResult(com.couchbase.client.protocol.run.Result.Builder result, ExistsResult value) {
        var builder = com.couchbase.client.protocol.sdk.kv.ExistsResult.newBuilder()
                .setCas(value.cas())
                .setExists(value.exists());

        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setExistsResult(builder));
    }

    public static com.google.protobuf.Duration convertDuration(Duration duration) {
        return com.google.protobuf.Duration.newBuilder()
                .setSeconds(duration.getSeconds())
                .setNanos(duration.getNano())
                .build();
    }

    public static com.couchbase.client.protocol.sdk.query.QueryMetaData.Builder convertMetaData(QueryMetaData metaData) {
        var out = com.couchbase.client.protocol.sdk.query.QueryMetaData.newBuilder()
                .setRequestId(metaData.requestId())
                .setClientContextId(metaData.clientContextId())
                .setStatus(
                        com.couchbase.client.protocol.sdk.query.QueryStatus.valueOf(
                                metaData.status().toString().toUpperCase()
                        )
                );

        if (metaData.signature().isPresent()) {
            out.setSignature(ByteString.copyFrom(metaData.signature().get().toBytes()));
        }

        for (QueryWarning warning: metaData.warnings()) {
            out.addWarnings(
                    com.couchbase.client.protocol.sdk.query.QueryWarning.newBuilder().setCode(warning.code()).setMessage(warning.message()).build()
            );
        }

        if (metaData.metrics().isPresent()) {
            var resultMetricsData = metaData.metrics().get();
            out.setMetrics(
                    com.couchbase.client.protocol.sdk.query.QueryMetrics.newBuilder()
                            .setElapsedTime(convertDuration(resultMetricsData.elapsedTime()))
                            .setExecutionTime(convertDuration(resultMetricsData.executionTime()))
                            .setSortCount(resultMetricsData.sortCount())
                            .setResultSize(resultMetricsData.resultSize())
                            .setResultCount(resultMetricsData.resultCount())
                            .setMutationCount(resultMetricsData.mutationCount())
                            .setErrorCount(resultMetricsData.errorCount())
                            .setWarningCount(resultMetricsData.warningCount())
            );
        }

        if (metaData.profile().isPresent()) {
            out.setProfile(
                    ByteString.copyFrom(metaData.profile().get().toBytes())
            );
        }

        return out;
    }

    public static void populateResult(com.couchbase.client.protocol.sdk.query.Command request, com.couchbase.client.protocol.run.Result.Builder result, QueryResult values) {

        var builder = com.couchbase.client.protocol.sdk.query.QueryResult.newBuilder();

        var content = ContentAsUtil.contentTypeList(request.getContentAs(),
                () -> values.rowsAs(byte[].class),
                () -> values.rowsAs(String.class),
                () -> values.rowsAs(JsonObject.class),
                () -> values.rowsAs(JsonArray.class),
                () -> values.rowsAs(Boolean.class),
                () -> values.rowsAs(Integer.class),
                () -> values.rowsAs(Double.class));

        if (content.isFailure()) {
          throw content.exception();
        }

        builder.addAllContent(content.value());

        // Metadata
        var convertedMetaData = convertMetaData(values.metaData());
        builder.setMetaData(convertedMetaData);

        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setQueryResult(builder));
    }

    public static Object contentOrMacro(com.couchbase.client.protocol.sdk.collection.mutatein.ContentOrMacro contentOrMacro) {
        switch (contentOrMacro.getContentOrMacroCase()) {
            case CONTENT -> {
                return content(contentOrMacro.getContent());
            }
            case MACRO -> {
                return convertMacro(contentOrMacro.getMacro());
            }
            default -> throw new UnsupportedOperationException("Unknown content type");
        }
    }

    public static Object content(Content content) {
        if (content.hasPassthroughString()) {
            return content.getPassthroughString();
        }
        else if (content.hasConvertToJson()) {
            return JsonObject.fromJson(content.getConvertToJson().toByteArray());
        }
        else if (content.hasByteArray()) {
            return content.getByteArray().toByteArray();
        }
        else if (content.hasNull()) {
            return null;
        }
        throw new UnsupportedOperationException("Unknown content type");
    }

    public static com.couchbase.client.java.kv.MutateInMacro convertMacro (MutateInMacro macro) {
        return switch (macro.name()) {
            case "CAS" -> com.couchbase.client.java.kv.MutateInMacro.CAS;
            case "SEQ_NO" -> com.couchbase.client.java.kv.MutateInMacro.SEQ_NO;
            case "VALUE_CRC_32C" -> com.couchbase.client.java.kv.MutateInMacro.VALUE_CRC_32C;
            default -> throw new UnsupportedOperationException("Macro value not supported: " + macro.name());
        };
    }

    public static @Nullable InsertOptions createOptions(com.couchbase.client.protocol.sdk.kv.Insert request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = InsertOptions.insertOptions();   
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasDurability()) convertDurability(opts.getDurability(), out);
            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasAbsoluteEpochSecs()) {
                    // [if:3.0.7]
                    out.expiry(Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs()));
                    // [else]
                    //? throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
                    // [end]
                }
                else if (opts.getExpiry().hasRelativeSecs()) out.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                else throw new UnsupportedOperationException("Unknown expiry");
            }
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    // [if:3.4.1]
    public static @Nullable ScanOptions createOptions(com.couchbase.client.protocol.sdk.kv.rangescan.Scan request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = ScanOptions.scanOptions();
            if (opts.hasIdsOnly()) out.idsOnly(opts.getIdsOnly());
            if (opts.hasConsistentWith()) out.consistentWith(convertMutationState(opts.getConsistentWith()));
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            if (opts.hasBatchByteLimit()) out.batchByteLimit(opts.getBatchByteLimit());
            if (opts.hasBatchItemLimit()) out.batchItemLimit(opts.getBatchItemLimit());
            // Presumably will be added soon, but not currently in Java SDK
            if (opts.hasBatchTimeLimit()) throw new UnsupportedOperationException();
            return out;
        }
        else return null;
    }
    // [end]

    public static @Nullable QueryOptions createOptions(com.couchbase.client.protocol.sdk.query.Command request, ConcurrentHashMap<String, RequestSpan> spans) {
        if(request.hasOptions()){
            var opts = request.getOptions();
            var out = QueryOptions.queryOptions();

            if (opts.hasScanConsistency()) {
                if (opts.getScanConsistency() == ScanConsistency.NOT_BOUNDED) out.scanConsistency(QueryScanConsistency.NOT_BOUNDED);
                else if (opts.getScanConsistency() == ScanConsistency.REQUEST_PLUS) out.scanConsistency(QueryScanConsistency.REQUEST_PLUS);
                else throw new UnsupportedOperationException("Unexpected scan consistency value:" + opts.getScanConsistency());
            }

            if (opts.hasConsistentWith()) {
                out.consistentWith(
                        MutationState.from(JsonObject.create()).add(new MutationToken(
                                (short) opts.getConsistentWith().getTokens(0).getPartitionId(),
                                opts.getConsistentWith().getTokens(0).getPartitionUuid(),
                                opts.getConsistentWith().getTokens(0).getSequenceNumber(),
                                opts.getConsistentWith().getTokens(0).getBucketName()
                        ))
                );
            }

            opts.getRawMap().forEach( (k,v) -> out.raw(k, JsonObject.fromJson(v)) );

            if (opts.getParametersNamedCount() > 0) {
                var outNamed = JsonObject.create();
                opts.getParametersNamedMap().forEach(outNamed::put);
                out.parameters(outNamed);
            } else {
                // named and positional parameters are mutually exclusive
                var outPos = JsonArray.create();
                opts.getParametersPositionalList().forEach(outPos::add);
                out.parameters(outPos);
            }

            if (opts.hasAdhoc()) out.adhoc(opts.getAdhoc());
            if (opts.hasProfile()) {
                if (opts.getProfile().equals("off")) out.profile(QueryProfile.OFF);
                else if (opts.getProfile().equals("phases")) out.profile(QueryProfile.PHASES);
                else if (opts.getProfile().equals("timings")) out.profile(QueryProfile.TIMINGS);
                else throw new UnsupportedOperationException("Query Option Profile: " + opts.getProfile() + " not supported");
            }
            if (opts.hasReadonly()) out.readonly(opts.getReadonly());

            // [if:3.0.9]
            if (opts.hasFlexIndex()) out.flexIndex(opts.getFlexIndex());
            // [end]
            if (opts.hasPipelineCap()) out.pipelineCap(opts.getPipelineCap());
            if (opts.hasPipelineBatch()) out.pipelineBatch(opts.getPipelineBatch());
            if (opts.hasScanCap()) out.scanCap(opts.getScanCap());
            if (opts.hasScanWaitMillis()) out.scanWait(Duration.ofMillis(opts.getScanWaitMillis()));
            if (opts.hasTimeoutMillis()) out.timeout(Duration.ofMillis(opts.getTimeoutMillis()));
            if (opts.hasMaxParallelism()) out.maxParallelism(opts.getMaxParallelism());
            if (opts.hasMetrics()) out.metrics(opts.getMetrics());
            if (opts.hasClientContextId()) out.clientContextId(opts.getClientContextId());
            // [if:3.2.5]
            if (opts.hasPreserveExpiry()) out.preserveExpiry(opts.getPreserveExpiry());
            // [end]
            // [if:3.4.8]
            if (opts.hasUseReplica()) out.useReplica(opts.getUseReplica());
            // [end]
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    public static @Nullable RemoveOptions createOptions(com.couchbase.client.protocol.sdk.kv.Remove request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = RemoveOptions.removeOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasDurability()) convertDurability(opts.getDurability(), out);
            if (opts.hasCas()) out.cas(opts.getCas());
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    public static @Nullable GetOptions createOptions(com.couchbase.client.protocol.sdk.kv.Get request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = GetOptions.getOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasWithExpiry()) out.withExpiry(opts.getWithExpiry());
            if (opts.getProjectionCount() > 0) out.project(opts.getProjectionList().stream().toList());
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    public static @Nullable GetAndLockOptions createOptions(com.couchbase.client.protocol.sdk.kv.GetAndLock request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = GetAndLockOptions.getAndLockOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    public static @Nullable UnlockOptions createOptions(com.couchbase.client.protocol.sdk.kv.Unlock request, ConcurrentHashMap<String, RequestSpan> spans) {

        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = UnlockOptions.unlockOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    public static @Nullable GetAndTouchOptions createOptions(com.couchbase.client.protocol.sdk.kv.GetAndTouch request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = GetAndTouchOptions.getAndTouchOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    public static @Nullable TouchOptions createOptions(com.couchbase.client.protocol.sdk.kv.Touch request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = TouchOptions.touchOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    public static @Nullable ExistsOptions createOptions(com.couchbase.client.protocol.sdk.kv.Exists request) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = ExistsOptions.existsOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            return out;
        }
        else return null;
    }

    public static @Nullable IncrementOptions createOptions(com.couchbase.client.protocol.sdk.kv.Increment request) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = IncrementOptions.incrementOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasDelta()) out.delta(opts.getDelta());
            if (opts.hasInitial()) out.initial(opts.getInitial());

            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasRelativeSecs()) out.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                else out.expiry(Duration.between(Instant.now(), Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs())));
            }

            if (opts.hasDurability()) convertDurability(opts.getDurability(), out);

            return out;
        }
        else return null;
    }

    public static @Nullable DecrementOptions createOptions(com.couchbase.client.protocol.sdk.kv.Decrement request) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = DecrementOptions.decrementOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasDelta()) out.delta(opts.getDelta());
            if (opts.hasInitial()) out.initial(opts.getInitial());

            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasRelativeSecs()) out.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                else out.expiry(Duration.between(Instant.now(), Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs())));
            }

            if (opts.hasDurability()) convertDurability(opts.getDurability(), out);

            return out;
        }
        else return null;
    }

    public static @Nullable AppendOptions createOptions(com.couchbase.client.protocol.sdk.kv.Append request) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = AppendOptions.appendOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasCas()) out.cas(opts.getCas());

            convertDurability(opts.getDurability(), out);

            return out;
        }
        else return null;
    }

    public static @Nullable PrependOptions createOptions(com.couchbase.client.protocol.sdk.kv.Prepend request) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = PrependOptions.prependOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasCas()) out.cas(opts.getCas());

            convertDurability(opts.getDurability(), out);

            return out;
        }
        else return null;
    }

    public static @Nullable GetAllReplicasOptions createOptions(com.couchbase.client.protocol.sdk.kv.GetAllReplicas request) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = GetAllReplicasOptions.getAllReplicasOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));

            return out;
        }
        else return null;
    }

    public static @Nullable GetAnyReplicaOptions createOptions(com.couchbase.client.protocol.sdk.kv.GetAnyReplica request) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = GetAnyReplicaOptions.getAnyReplicaOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));

            return out;
        }
        else return null;
    }
    public static @Nullable MutateInOptions createOptions(com.couchbase.client.protocol.sdk.collection.mutatein.MutateIn request) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = MutateInOptions.mutateInOptions();

            if (opts.hasTimeoutMillis()) out.timeout(Duration.ofMillis(opts.getTimeoutMillis()));
            if (opts.hasAccessDeleted()) out.accessDeleted(opts.getAccessDeleted());
            if (opts.hasCas()) out.cas(opts.getCas());
            // [if:3.0.1]
            if (opts.hasCreateAsDeleted()) out.createAsDeleted(opts.getCreateAsDeleted());
            // [end]
            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasAbsoluteEpochSecs()) {
                    // [if:3.0.7]
                    out.expiry(Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs()));
                    // [else]
                    //? throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
                    // [end]
                }
                else if (opts.getExpiry().hasRelativeSecs()) out.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                else throw new UnsupportedOperationException("Unknown expiry");
            }
            if (opts.hasDurability()) convertDurability(opts.getDurability(), out);
            // [if:3.1.5]
            if (opts.hasPreserveExpiry()) out.preserveExpiry(opts.getPreserveExpiry());
            // [end]
            if (opts.hasStoreSemantics()) {
                switch (opts.getStoreSemantics()) {
                    case INSERT -> out.storeSemantics(StoreSemantics.INSERT);
                    case UPSERT -> out.storeSemantics(StoreSemantics.UPSERT);
                    case REPLACE -> out.storeSemantics(StoreSemantics.REPLACE);
                    case UNRECOGNIZED -> throw new UnsupportedOperationException("Unrecognised store semantics option value: " + opts.getStoreSemantics());
                }
            }

            return out;
        }
        else return null;
    }

    public static @Nullable ReplaceOptions createOptions(com.couchbase.client.protocol.sdk.kv.Replace request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = ReplaceOptions.replaceOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasDurability()) convertDurability(opts.getDurability(), out);
            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasAbsoluteEpochSecs()) {
                    // [if:3.0.7]
                    out.expiry(Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs()));
                    // [else]
                    //? throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
                    // [end]
                }
                else if (opts.getExpiry().hasRelativeSecs()) out.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                else throw new UnsupportedOperationException("Unknown expiry");
            }
            if (opts.hasPreserveExpiry()) {
                // [if:3.1.5]
                out.preserveExpiry(opts.getPreserveExpiry());
                // [else]
                //? throw new UnsupportedOperationException();
                // [end]
            }
            if (opts.hasCas()) out.cas(opts.getCas());
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    public static @Nullable UpsertOptions createOptions(com.couchbase.client.protocol.sdk.kv.Upsert request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = UpsertOptions.upsertOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasDurability()) convertDurability(opts.getDurability(), out);
            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasAbsoluteEpochSecs()) {
                    // [if:3.0.7]
                    out.expiry(Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs()));
                    // [else]
                    //? throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
                    // [end]
                }
                else if (opts.getExpiry().hasRelativeSecs()) out.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                else throw new UnsupportedOperationException("Unknown expiry");
            }
            if (opts.hasPreserveExpiry()) {
                // [if:3.1.5]
                out.preserveExpiry(opts.getPreserveExpiry());
                // [else]
                //? throw new UnsupportedOperationException();
                // [end]
            }
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    public static void convertDurability(com.couchbase.client.protocol.shared.DurabilityType durability, CommonDurabilityOptions options) {
        if (durability.hasDurabilityLevel()) {
            options.durability(switch (durability.getDurabilityLevel()) {
                case NONE -> DurabilityLevel.NONE;
                case MAJORITY -> DurabilityLevel.MAJORITY;
                case MAJORITY_AND_PERSIST_TO_ACTIVE -> DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
                case PERSIST_TO_MAJORITY -> DurabilityLevel.PERSIST_TO_MAJORITY;
                default -> throw new UnsupportedOperationException("Unexpected value: " + durability.getDurabilityLevel());
            });
        }
        else if (durability.hasObserve()) {
            options.durability(switch (durability.getObserve().getPersistTo()) {
                        case PERSIST_TO_NONE -> PersistTo.NONE;
                        case PERSIST_TO_ACTIVE -> PersistTo.ACTIVE;
                        case PERSIST_TO_ONE -> PersistTo.ONE;
                        case PERSIST_TO_TWO -> PersistTo.TWO;
                        case PERSIST_TO_THREE -> PersistTo.THREE;
                        case PERSIST_TO_FOUR -> PersistTo.FOUR;
                        default -> throw new UnsupportedOperationException("Unexpected value: " + durability.getDurabilityLevel());
                    }, switch (durability.getObserve().getReplicateTo()) {
                        case REPLICATE_TO_NONE -> ReplicateTo.NONE;
                        case REPLICATE_TO_ONE -> ReplicateTo.ONE;
                        case REPLICATE_TO_TWO -> ReplicateTo.TWO;
                        case REPLICATE_TO_THREE -> ReplicateTo.THREE;
                        default -> throw new UnsupportedOperationException("Unexpected value: " + durability.getDurabilityLevel());
                    });
        }
        else {
            throw new UnsupportedOperationException("Unknown durability");
        }
    }

    public static Transcoder convertTranscoder(com.couchbase.client.protocol.shared.Transcoder transcoder) {
        if (transcoder.hasRawJson()) return RawJsonTranscoder.INSTANCE;
        if (transcoder.hasJson()) return JSON_TRANSCODER;
        if (transcoder.hasLegacy()) return LEGACY_TRANSCODER;
        if (transcoder.hasRawString()) return RawStringTranscoder.INSTANCE;
        if (transcoder.hasRawBinary()) return RawBinaryTranscoder.INSTANCE;
        throw new UnsupportedOperationException("Unknown transcoder");
    }

    public static MutationState convertMutationState(com.couchbase.client.protocol.shared.MutationState consistentWith) {
        var mutationTokens = consistentWith.getTokensList().stream()
                .map(mt -> new com.couchbase.client.core.msg.kv.MutationToken((short) mt.getPartitionId(),
                        mt.getPartitionUuid(),
                        mt.getSequenceNumber(),
                        mt.getBucketName()))
                .toList();
        return MutationState.from(mutationTokens.toArray(new com.couchbase.client.core.msg.kv.MutationToken[0]));
    }

  public static WaitUntilReadyOptions waitUntilReadyOptions(WaitUntilReadyRequest request) {

    var options = WaitUntilReadyOptions.waitUntilReadyOptions();

    if (request.getOptions().hasDesiredState()) {
      options.desiredState(ClusterState.valueOf(request.getOptions().getDesiredState().toString()));
    }

    if (request.getOptions().getServiceTypesList().size() > 0) {

      var serviceTypes = request.getOptions().getServiceTypesList();

      var services = new HashSet<ServiceType>();
      for (com.couchbase.client.protocol.sdk.cluster.waituntilready.ServiceType service : serviceTypes) {
        var newService = com.couchbase.client.core.service.ServiceType.valueOf(service.toString());
        services.add(newService);

      }

      options.serviceTypes(services);
    }

    return options;
  }
}

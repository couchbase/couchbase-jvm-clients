/*
 * Copyright 2023 Couchbase, Inc.
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
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
// [if:3.4.9]
import com.couchbase.client.java.kv.LookupInAllReplicasOptions;
import com.couchbase.client.java.kv.LookupInAnyReplicaOptions;
import com.couchbase.client.java.kv.LookupInReplicaResult;
// [end]
import com.couchbase.client.java.kv.LookupInOptions;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.performer.core.perf.PerRun;
import com.couchbase.client.performer.core.stream.StreamStreamer;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.CollectionLevelCommand;
import com.couchbase.client.protocol.shared.DocLocation;
import com.couchbase.stream.FluxStreamer;
import com.couchbase.utils.ClusterConnection;
import com.couchbase.utils.ContentAsUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.couchbase.JavaSdkCommandExecutor.convertExceptionShared;
import static com.couchbase.JavaSdkCommandExecutor.setSuccess;
import static com.couchbase.client.performer.core.util.TimeUtil.getTimeNow;

public class LookupInHelper {
  public static Result.Builder handleLookupIn(PerRun perRun,
                                              ClusterConnection connection,
                                              com.couchbase.client.protocol.sdk.Command command,
                                              Function<DocLocation, String> docId,
                                              ConcurrentHashMap<String, RequestSpan> spans) {
    var clc = command.getCollectionCommand();
    var out = com.couchbase.client.protocol.run.Result.newBuilder();
    out.setInitiated(getTimeNow());

    if (clc.hasLookupIn()) {
      handleLookupIn(connection, command, docId, clc, out, spans);
    } else if (clc.hasLookupInAllReplicas()) {
      handleLookupInAllReplicas(perRun, connection, command, docId, clc, out, spans);
    } else if (clc.hasLookupInAnyReplica()) {
      handleLookupInAnyReplica(connection, command, docId, clc, out, spans);
    }

    return out;
  }

  public static Mono<Result> handleLookupInReactive(PerRun perRun,
                                                    ClusterConnection connection,
                                                    com.couchbase.client.protocol.sdk.Command command,
                                                    Function<DocLocation, String> docId,
                                                    ConcurrentHashMap<String, RequestSpan> spans) {
    var clc = command.getCollectionCommand();
    var out = com.couchbase.client.protocol.run.Result.newBuilder();
    out.setInitiated(getTimeNow());

    Mono<?> result;

    if (clc.hasLookupIn()) {
      result = handleLookupInReactive(connection, command, docId, clc, out, spans);
    } else if (clc.hasLookupInAllReplicas()) {
      result = handleLookupInAllReplicasReactive(perRun, connection, command, docId, clc, out, spans);
    } else {
      if (!clc.hasLookupInAnyReplica()) {
        throw new UnsupportedOperationException();
      }
      result = handleLookupInAnyReplicaReactive(connection, command, docId, clc, out, spans);
    }

    return result.map(v -> out.build());
  }

  private static void handleLookupIn(
          ClusterConnection connection,
          com.couchbase.client.protocol.sdk.Command command,
          Function<DocLocation, String> getId,
          CollectionLevelCommand clc,
          Result.Builder out,
          ConcurrentHashMap<String, RequestSpan> spans) {
    var req = clc.getLookupIn();
    var collection = connection.collection(req.getLocation());
    var options = createOptions(req, spans);
    var docId = getId.apply(req.getLocation());

    var specs = mapSpecs(req.getSpecList());

    var start = System.nanoTime();

    var exists = req.getSpecList().get(0).getExists();
    var str = exists.toString();
    System.out.println(str);

    LookupInResult result;
    if (options != null) {
      result = collection.lookupIn(docId, specs, options);
    } else {
      result = collection.lookupIn(docId, specs);
    }

    out.setElapsedNanos(System.nanoTime() - start);

    if (command.getReturnResult()) {
      populateResult(req, out, result);
    } else {
      setSuccess(out);
    }
  }

  private static void handleLookupInAnyReplica(
          ClusterConnection connection,
          com.couchbase.client.protocol.sdk.Command command,
          Function<DocLocation, String> getId,
          CollectionLevelCommand clc,
          Result.Builder out,
          ConcurrentHashMap<String, RequestSpan> spans) {
    // [if:<3.4.9]
    //? throw new UnsupportedOperationException("This version of the SDK does not support lookupInAllReplicas");
    // [else]
    var req = clc.getLookupInAnyReplica();
    var collection = connection.collection(req.getLocation());
    var options = createOptions(req, spans);
    var docId = getId.apply(req.getLocation());

    var specs = mapSpecs(req.getSpecList());

    var start = System.nanoTime();

    LookupInReplicaResult result;
    if (options != null) {
      result = collection.lookupInAnyReplica(docId, specs, options);
    } else {
      result = collection.lookupInAnyReplica(docId, specs);
    }

    out.setElapsedNanos(System.nanoTime() - start);

    if (command.getReturnResult()) {
      var r = populateResult(req.getSpecList(), result);
      out.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder().setLookupInAnyReplicaResult(r));
    } else {
      setSuccess(out);
    }
    // [end]
  }

  private static void handleLookupInAllReplicas(
          PerRun perRun,
          ClusterConnection connection,
          com.couchbase.client.protocol.sdk.Command command,
          Function<DocLocation, String> getId,
          CollectionLevelCommand clc,
          Result.Builder out,
          ConcurrentHashMap<String, RequestSpan> spans) {
    // [if:<3.4.9]
    //? throw new UnsupportedOperationException("This version of the SDK does not support lookupInAllReplicas");
    // [else]
    var req = clc.getLookupInAllReplicas();
    var collection = connection.collection(req.getLocation());
    var options = createOptions(req, spans);
    var docId = getId.apply(req.getLocation());

    var specs = mapSpecs(req.getSpecList());

    var start = System.nanoTime();

    Stream<LookupInReplicaResult> stream;
    if (options != null) {
      stream = collection.lookupInAllReplicas(docId, specs, options);
    } else {
      stream = collection.lookupInAllReplicas(docId, specs);
    }

    out.setElapsedNanos(System.nanoTime() - start);

    var streamer = new StreamStreamer<LookupInReplicaResult>(
            stream,
            perRun,
            req.getStreamConfig().getStreamId(),
            req.getStreamConfig(),
            (r) -> {
              return com.couchbase.client.protocol.run.Result.newBuilder()
                      .setSdk(
                              com.couchbase.client.protocol.sdk.Result.newBuilder()
                                      .setLookupInAllReplicasResult(
                                              com.couchbase.client.protocol.sdk.kv.lookupin.LookupInAllReplicasResult.newBuilder()
                                                      .setStreamId(req.getStreamConfig().getStreamId())
                                                      .setLookupInReplicaResult(populateResult(req.getSpecList(), r))))
                      .build();
            },
            JavaSdkCommandExecutor::convertExceptionShared);

    perRun.streamerOwner().addAndStart(streamer);
    out.setStream(
            com.couchbase.client.protocol.streams.Signal.newBuilder()
                    .setCreated(
                            com.couchbase.client.protocol.streams.Created.newBuilder()
                                    .setType(com.couchbase.client.protocol.streams.Type.STREAM_KV_RANGE_SCAN)
                                    .setStreamId(streamer.streamId())));
    // [end]
  }

  private static Mono<?> handleLookupInReactive(
          ClusterConnection connection,
          com.couchbase.client.protocol.sdk.Command command,
          Function<DocLocation, String> getId,
          CollectionLevelCommand clc,
          Result.Builder out,
          ConcurrentHashMap<String, RequestSpan> spans) {
    var req = clc.getLookupIn();
    var collection = connection.collection(req.getLocation()).reactive();
    var options = createOptions(req, spans);
    var docId = getId.apply(req.getLocation());

    var specs = mapSpecs(req.getSpecList());

    var start = System.nanoTime();

    Mono<LookupInResult> result;
    if (options != null) {
      result = collection.lookupIn(docId, specs, options);
    } else {
      result = collection.lookupIn(docId, specs);
    }

    return result.doOnNext(r -> {
      out.setElapsedNanos(System.nanoTime() - start);

      if (command.getReturnResult()) {
        populateResult(req, out, r);
      } else {
        setSuccess(out);
      }
    });
  }

  private static Mono<?> handleLookupInAnyReplicaReactive(
          ClusterConnection connection,
          com.couchbase.client.protocol.sdk.Command command,
          Function<DocLocation, String> getId,
          CollectionLevelCommand clc,
          Result.Builder out,
          ConcurrentHashMap<String, RequestSpan> spans) {
    // [if:<3.4.9]
    //? throw new UnsupportedOperationException("This version of the SDK does not support lookupInAnyReplica");
    // [else]
    var req = clc.getLookupInAnyReplica();
    var collection = connection.collection(req.getLocation()).reactive();
    var options = createOptions(req, spans);
    var docId = getId.apply(req.getLocation());

    var specs = mapSpecs(req.getSpecList());

    var start = System.nanoTime();

    Mono<LookupInReplicaResult> result;
    if (options != null) {
      result = collection.lookupInAnyReplica(docId, specs, options);
    } else {
      result = collection.lookupInAnyReplica(docId, specs);
    }

    return result
            .doOnError(err -> err.printStackTrace())
            .doFinally(v -> System.out.println("Finished"))
            .doOnNext(v -> {
              System.out.println("Got next");
              out.setElapsedNanos(System.nanoTime() - start);

              if (command.getReturnResult()) {
                var r = populateResult(req.getSpecList(), v);
                out.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder().setLookupInAnyReplicaResult(r));
              } else {
                setSuccess(out);
              }
            });
    // [end]
  }

  private static Mono<?> handleLookupInAllReplicasReactive(
          PerRun perRun,
          ClusterConnection connection,
          com.couchbase.client.protocol.sdk.Command command,
          Function<DocLocation, String> getId,
          CollectionLevelCommand clc,
          Result.Builder out,
          ConcurrentHashMap<String, RequestSpan> spans) {
    // [if:<3.4.9]
    //? throw new UnsupportedOperationException("This version of the SDK does not support lookupInAllReplicas");
    // [else]
    var req = clc.getLookupInAllReplicas();
    var collection = connection.collection(req.getLocation()).reactive();
    var options = createOptions(req, spans);
    var docId = getId.apply(req.getLocation());

    var specs = mapSpecs(req.getSpecList());

    var start = System.nanoTime();

    Flux<LookupInReplicaResult> results;
    if (options != null) {
      results = collection.lookupInAllReplicas(docId, specs, options);
    } else {
      results = collection.lookupInAllReplicas(docId, specs);
    }

    out.setElapsedNanos(System.nanoTime() - start);

    var streamer = new FluxStreamer<LookupInReplicaResult>(
            results,
            perRun,
            req.getStreamConfig().getStreamId(),
            req.getStreamConfig(),
            (r) -> {
              return com.couchbase.client.protocol.run.Result.newBuilder()
                      .setSdk(
                              com.couchbase.client.protocol.sdk.Result.newBuilder()
                                      .setLookupInAllReplicasResult(
                                              com.couchbase.client.protocol.sdk.kv.lookupin.LookupInAllReplicasResult.newBuilder()
                                                      .setStreamId(req.getStreamConfig().getStreamId())
                                                      .setLookupInReplicaResult(populateResult(req.getSpecList(), r))))
                      .build();
            },
            JavaSdkCommandExecutor::convertExceptionShared
    );
    perRun.streamerOwner().addAndStart(streamer);
    out.setStream(
            com.couchbase.client.protocol.streams.Signal.newBuilder()
                    .setCreated(
                            com.couchbase.client.protocol.streams.Created.newBuilder()
                                    .setType(com.couchbase.client.protocol.streams.Type.STREAM_KV_RANGE_SCAN)
                                    .setStreamId(streamer.streamId())));

    return Mono.just(0);
    // [end]
  }

  private static List<com.couchbase.client.java.kv.LookupInSpec> mapSpecs(List<com.couchbase.client.protocol.sdk.kv.lookupin.LookupInSpec> specList) {
    return specList.stream().map(v -> {
      if (v.hasExists()) {
        var out = LookupInSpec.exists(v.getExists().getPath());
        if (v.getExists().hasXattr() && v.getExists().getXattr()) {
          out = out.xattr();
        }
        return (LookupInSpec) out;
      } else if (v.hasGet()) {
        var out = LookupInSpec.get(v.getGet().getPath());
        if (v.getGet().hasXattr() && v.getGet().getXattr()) {
          out = out.xattr();
        }
        return (LookupInSpec) out;
      } else if (v.hasCount()) {
        var out = LookupInSpec.count(v.getCount().getPath());
        if (v.getCount().hasXattr() && v.getCount().getXattr()) {
          out = out.xattr();
        }
        return (LookupInSpec) out;
      } else {
        throw new UnsupportedOperationException("Unknown spec " + v);
      }
    }).toList();
  }

  private static @Nullable LookupInOptions createOptions(com.couchbase.client.protocol.sdk.kv.lookupin.LookupIn request,
                                                         ConcurrentHashMap<String, RequestSpan> spans) {
    if (request.hasOptions()) {
      var opts = request.getOptions();
      var out = LookupInOptions.lookupInOptions();
      if (opts.hasTimeoutMillis()) {
        out = out.timeout(Duration.ofMillis(opts.getTimeoutMillis()));
      }
      if (opts.hasAccessDeleted()) {
        out = out.accessDeleted(opts.getAccessDeleted());
      }
      if (opts.hasParentSpanId()) {
        out = out.parentSpan(spans.get(opts.getParentSpanId()));
      }
      return out;
    } else {
      return null;
    }
  }

  // [if:3.4.9]
  private static @Nullable LookupInAnyReplicaOptions createOptions(com.couchbase.client.protocol.sdk.kv.lookupin.LookupInAnyReplica request,
                                                                   ConcurrentHashMap<String, RequestSpan> spans) {
    if (request.hasOptions()) {
      var opts = request.getOptions();
      var out = LookupInAnyReplicaOptions.lookupInAnyReplicaOptions();
      if (opts.hasTimeoutMillis()) {
        out = out.timeout(Duration.ofMillis(opts.getTimeoutMillis()));
      }
      if (opts.hasParentSpanId()) {
        out = out.parentSpan(spans.get(opts.getParentSpanId()));
      }
      return out;
    } else {
      return null;
    }
  }

  private static @Nullable LookupInAllReplicasOptions createOptions(com.couchbase.client.protocol.sdk.kv.lookupin.LookupInAllReplicas request,
                                                                    ConcurrentHashMap<String, RequestSpan> spans) {
    if (request.hasOptions()) {
      var opts = request.getOptions();
      var out = LookupInAllReplicasOptions.lookupInAllReplicasOptions();
      if (opts.hasTimeoutMillis()) {
        out = out.timeout(Duration.ofMillis(opts.getTimeoutMillis()));
      }
      if (opts.hasParentSpanId()) {
        out = out.parentSpan(spans.get(opts.getParentSpanId()));
      }
      return out;
    } else {
      return null;
    }
  }
  // [end]

  private static void populateResult(com.couchbase.client.protocol.sdk.kv.lookupin.LookupIn request,
                                     com.couchbase.client.protocol.run.Result.Builder out,
                                     LookupInResult result) {
    var specs = new ArrayList<com.couchbase.client.protocol.sdk.kv.lookupin.LookupInSpecResult>();

    for (int i = 0; i < request.getSpecCount(); i++) {
      var spec = request.getSpec(i);
      final int x = i;

      com.couchbase.client.protocol.sdk.kv.lookupin.BooleanOrError existsResult;
      try {
        var exists = result.exists(i);
        existsResult = com.couchbase.client.protocol.sdk.kv.lookupin.BooleanOrError.newBuilder()
                .setValue(exists)
                .build();
      } catch (Throwable err) {
        existsResult = com.couchbase.client.protocol.sdk.kv.lookupin.BooleanOrError.newBuilder()
                .setException(convertExceptionShared(err))
                .build();
      }

      var contentAs = spec.getContentAs();

      var content = ContentAsUtil.contentType(
              contentAs,
              () -> result.contentAs(x, byte[].class),
              () -> result.contentAs(x, String.class),
              () -> result.contentAs(x, JsonObject.class),
              () -> result.contentAs(x, JsonArray.class),
              () -> result.contentAs(x, Boolean.class),
              () -> result.contentAs(x, Integer.class),
              () -> result.contentAs(x, Double.class));

      com.couchbase.client.protocol.shared.ContentOrError contentResult;
      if (content.isFailure()) {
        contentResult = com.couchbase.client.protocol.shared.ContentOrError.newBuilder()
                .setException(convertExceptionShared(content.exception()))
                .build();
      } else {
        contentResult = com.couchbase.client.protocol.shared.ContentOrError.newBuilder()
                .setContent(content.value())
                .build();
      }

      specs.add(com.couchbase.client.protocol.sdk.kv.lookupin.LookupInSpecResult.newBuilder()
              .setExistsResult(existsResult)
              .setContentAsResult(contentResult)
              .build());
    }

    out.setSdk(
            com.couchbase.client.protocol.sdk.Result.newBuilder()
                    .setLookupInResult(
                            com.couchbase.client.protocol.sdk.kv.lookupin.LookupInResult.newBuilder()
                                    .setCas(result.cas())
                                    .addAllResults(specs)));
  }

  // [if:3.4.9]
  private static com.couchbase.client.protocol.sdk.kv.lookupin.LookupInReplicaResult populateResult(List<com.couchbase.client.protocol.sdk.kv.lookupin.LookupInSpec> specs,
                                                                                                    LookupInReplicaResult result) {
    var specResults = new ArrayList<com.couchbase.client.protocol.sdk.kv.lookupin.LookupInSpecResult>();

    for (int i = 0; i < specs.size(); i++) {
      var spec = specs.get(i);
      final int x = i;

      com.couchbase.client.protocol.sdk.kv.lookupin.BooleanOrError existsResult;
      try {
        var exists = result.exists(i);
        existsResult = com.couchbase.client.protocol.sdk.kv.lookupin.BooleanOrError.newBuilder()
                .setValue(exists)
                .build();
      } catch (Throwable err) {
        existsResult = com.couchbase.client.protocol.sdk.kv.lookupin.BooleanOrError.newBuilder()
                .setException(convertExceptionShared(err))
                .build();
      }

      var contentAs = spec.getContentAs();

      var content = ContentAsUtil.contentType(
              contentAs,
              () -> result.contentAs(x, byte[].class),
              () -> result.contentAs(x, String.class),
              () -> result.contentAs(x, JsonObject.class),
              () -> result.contentAs(x, JsonArray.class),
              () -> result.contentAs(x, Boolean.class),
              () -> result.contentAs(x, Integer.class),
              () -> result.contentAs(x, Double.class));

      com.couchbase.client.protocol.shared.ContentOrError contentResult;
      if (content.isFailure()) {
        contentResult = com.couchbase.client.protocol.shared.ContentOrError.newBuilder()
                .setException(convertExceptionShared(content.exception()))
                .build();
      } else {
        contentResult = com.couchbase.client.protocol.shared.ContentOrError.newBuilder()
                .setContent(content.value())
                .build();
      }

      specResults.add(com.couchbase.client.protocol.sdk.kv.lookupin.LookupInSpecResult.newBuilder()
              .setExistsResult(existsResult)
              .setContentAsResult(contentResult)
              .build());
    }

    return com.couchbase.client.protocol.sdk.kv.lookupin.LookupInReplicaResult.newBuilder()
            .setIsReplica(result.isReplica())
            .setCas(result.cas())
            .addAllResults(specResults)
            .build();
  }
  // [end]

}

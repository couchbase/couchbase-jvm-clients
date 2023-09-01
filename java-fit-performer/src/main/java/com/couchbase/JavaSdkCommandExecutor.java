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
import com.couchbase.client.java.kv.CommonDurabilityOptions;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.query.*;
import com.couchbase.client.protocol.sdk.kv.Get;
// [start:3.2.1]
import com.couchbase.eventing.EventingHelper;
// [end:3.2.1]
// [start:3.2.4]
import com.couchbase.manager.BucketManagerHelper;
// [end:3.2.4]
// [start:3.4.1]
import com.couchbase.client.java.kv.ScanOptions;
import com.couchbase.client.java.kv.ScanResult;
// [end:3.4.1]
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.performer.core.commands.SdkCommandExecutor;
import com.couchbase.client.performer.core.perf.Counters;
import com.couchbase.client.performer.core.perf.PerRun;
import com.couchbase.client.performer.core.stream.StreamStreamer;
import com.couchbase.client.performer.core.util.ErrorUtil;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.cluster.waituntilready.WaitUntilReadyRequest;
import com.couchbase.client.protocol.sdk.kv.rangescan.Scan;
import com.couchbase.client.protocol.shared.Content;
import com.couchbase.client.protocol.shared.CouchbaseExceptionEx;
import com.couchbase.client.protocol.shared.CouchbaseExceptionType;
import com.couchbase.client.protocol.shared.Exception;
import com.couchbase.client.protocol.shared.ExceptionOther;
import com.couchbase.client.protocol.shared.ScanConsistency;
// [start:3.4.3]
import com.couchbase.query.QueryIndexManagerHelper;
// [end:3.4.3]
import com.couchbase.utils.ClusterConnection;
import com.couchbase.utils.ContentAsUtil;
import com.google.protobuf.ByteString;
// [start:3.4.5]
import com.couchbase.search.SearchHelper;
import static com.couchbase.search.SearchHelper.handleSearchBlocking;
// [end:3.4.5]

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static com.couchbase.client.performer.core.util.TimeUtil.getTimeNow;
import static com.couchbase.client.protocol.streams.Type.STREAM_KV_RANGE_SCAN;


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
            if (op.getReturnResult()) populateResult(request, result, gr);
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
        // [start:3.4.1]
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
        // [end:3.4.1]
        } else if (op.hasClusterCommand()) {
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
          // [start:3.2.4]
          else if (clc.hasBucketManager()) {
            BucketManagerHelper.handleBucketManger(connection.cluster(), spans, op, result);
          }
          // [end:3.2.4]
          // [start:3.2.1]
          else if (clc.hasEventingFunctionManager()) {
            EventingHelper.handleEventingFunctionManager(connection.cluster(), spans, op, result);
          }
          // [end:3.2.1]

            // [start:3.4.3]
            if (clc.hasQueryIndexManager()) {
              QueryIndexManagerHelper.handleClusterQueryIndexManager(connection.cluster(), spans, op, result);
            }
            // [end:3.4.3]

            // [start:3.4.5]
            if (clc.hasSearch()) {
                com.couchbase.client.protocol.sdk.search.Search command = clc.getSearch();
                return handleSearchBlocking(connection.cluster(), null, spans, command);
            }
            else if (clc.hasSearchIndexManager()) {
              return SearchHelper.handleClusterSearchIndexManager(connection.cluster(), spans, op);
            }
            // [end:3.4.5]

            if (clc.hasQuery()) {
                var query = clc.getQuery().getStatement();
                QueryResult qr;
                if (clc.getQuery().hasOptions()) {
                    qr = connection.cluster().query(query, createOptions(clc.getQuery()));
                }
                else {
                    qr = connection.cluster().query(query);
                }
                populateResult(clc.getQuery(), result, qr);
            }

        } else if (op.hasBucketCommand()) {
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

        } else if (op.hasScopeCommand()) {
          var slc = op.getScopeCommand();
          var scope = connection.cluster().bucket(slc.getScope().getBucketName()).scope(slc.getScope().getScopeName());

          // [start:3.0.9]
          if (slc.hasQuery()) {
            var query = slc.getQuery().getStatement();
            QueryResult qr;
            if (slc.getQuery().hasOptions()) qr = scope.query(query, createOptions(slc.getQuery()));
            else qr = scope.query(query);
            populateResult(slc.getQuery(), result, qr);
          }
          // [end:3.0.9]

          // [start:3.4.5]
          if (slc.hasSearch()) {
            com.couchbase.client.protocol.sdk.search.Search command = slc.getSearch();
            return handleSearchBlocking(connection.cluster(), scope, spans, command);
          } else if (slc.hasSearchIndexManager()) {
            return SearchHelper.handleScopeSearchIndexManager(scope, spans, op);
          }
          // [end:3.4.5]
        } else if (op.hasCollectionCommand()) {
          var clc = op.getCollectionCommand();

          Collection collection = null;
          if (clc.hasCollection()) {
            collection = connection.cluster()
                    .bucket(clc.getCollection().getBucketName())
                    .scope(clc.getCollection().getScopeName())
                    .collection(clc.getCollection().getCollectionName());
          }

            // [start:3.4.3]
            if (clc.hasQueryIndexManager()) {
                QueryIndexManagerHelper.handleCollectionQueryIndexManager(collection, spans, op, result);
            }
            // [end:3.4.3]
            if (clc.hasLookupIn() || clc.hasLookupInAllReplicas() || clc.hasLookupInAnyReplica()) {
                result = LookupInHelper.handleLookupIn(perRun, connection, op, this::getDocId, spans);
            }
        } else {
            throw new UnsupportedOperationException(new IllegalArgumentException("Unknown operation"));
        }

        return result.build();
    }

    // [start:3.4.1]
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
    // [end:3.4.1]

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

  public static void populateResult(Get req, Result.Builder result, GetResult value) {
    var content = ContentAsUtil.contentType(req.getContentAs(),
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

        // [start:3.0.7]
        value.expiryTime().ifPresent(et -> builder.setExpiryTime(et.getEpochSecond()));
        // [end:3.0.7]

        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setGetResult(builder));
      }
      else {
        throw content.exception();
      }
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

    public static Object content(Content content) {
        if (content.hasPassthroughString()) {
            return content.getPassthroughString();
        }
        else if (content.hasConvertToJson()) {
            return JsonObject.fromJson(content.getConvertToJson().toByteArray());
        }
        throw new UnsupportedOperationException("Unknown content type");
    }

    public static @Nullable InsertOptions createOptions(com.couchbase.client.protocol.sdk.kv.Insert request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = InsertOptions.insertOptions();   
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasDurability()) convertDurability(opts.getDurability(), out);
            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasAbsoluteEpochSecs()) {
                    // [start:3.0.7]
                    out.expiry(Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs()));
                    // [end:3.0.7]
                    // [start:<3.0.7]
/*
                    throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
                    // [end:<3.0.7]
*/
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

    // [start:3.4.1]
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
    // [end:3.4.1]

    public static @Nullable QueryOptions createOptions(com.couchbase.client.protocol.sdk.query.Command request) {
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

            // [start:3.0.9]
            if (opts.hasFlexIndex()) out.flexIndex(opts.getFlexIndex());
            // [end:3.0.9]
            if (opts.hasPipelineCap()) out.pipelineCap(opts.getPipelineCap());
            if (opts.hasPipelineBatch()) out.pipelineBatch(opts.getPipelineBatch());
            if (opts.hasScanCap()) out.scanCap(opts.getScanCap());
            if (opts.hasScanWaitMillis()) out.scanWait(Duration.ofMillis(opts.getScanWaitMillis()));
            if (opts.hasTimeoutMillis()) out.timeout(Duration.ofMillis(opts.getTimeoutMillis()));
            if (opts.hasMaxParallelism()) out.maxParallelism(opts.getMaxParallelism());
            if (opts.hasMetrics()) out.metrics(opts.getMetrics());
            if (opts.hasClientContextId()) out.clientContextId(opts.getClientContextId());
            // [start:3.2.5]
            if (opts.hasPreserveExpiry()) out.preserveExpiry(opts.getPreserveExpiry());
            // [end:3.2.5]
            // [start:3.4.8]
            if (opts.hasUseReplica()) out.useReplica(opts.getUseReplica());
            // [end:3.4.8]
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

    public static @Nullable ReplaceOptions createOptions(com.couchbase.client.protocol.sdk.kv.Replace request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = ReplaceOptions.replaceOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasDurability()) convertDurability(opts.getDurability(), out);
            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasAbsoluteEpochSecs()) {
                    // [start:3.0.7]
                    out.expiry(Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs()));
                    // [end:3.0.7]
                    // [start:<3.0.7]
/*
                    throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
                    // [end:<3.0.7]
*/
                }
                else if (opts.getExpiry().hasRelativeSecs()) out.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                else throw new UnsupportedOperationException("Unknown expiry");
            }
            if (opts.hasPreserveExpiry()) {
                // [start:3.1.5]
                out.preserveExpiry(opts.getPreserveExpiry());
                // [end:3.1.5]
                // [start:<3.1.5]
/*
                throw new UnsupportedOperationException();
                // [end:<3.1.5]
*/
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
                    // [start:3.0.7]
                    out.expiry(Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs()));
                    // [end:3.0.7]
                    // [start:<3.0.7]
/*
                    throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
                    // [end:<3.0.7]
*/
                }
                else if (opts.getExpiry().hasRelativeSecs()) out.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                else throw new UnsupportedOperationException("Unknown expiry");
            }
            if (opts.hasPreserveExpiry()) {
                // [start:3.1.5]
                out.preserveExpiry(opts.getPreserveExpiry());
                // [end:3.1.5]
                // [start:<3.1.5]
/*
                throw new UnsupportedOperationException();
                // [end:<3.1.5]
*/
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

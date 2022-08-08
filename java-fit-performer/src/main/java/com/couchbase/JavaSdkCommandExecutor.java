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

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.codec.DefaultJsonSerializer;
import com.couchbase.client.java.codec.JsonTranscoder;
import com.couchbase.client.java.codec.LegacyTranscoder;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.codec.RawStringTranscoder;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.CommonDurabilityOptions;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.performer.core.commands.SdkCommandExecutor;
import com.couchbase.client.performer.core.perf.Counters;
import com.couchbase.client.performer.core.util.ErrorUtil;
import com.couchbase.client.protocol.shared.*;
import com.couchbase.client.protocol.shared.Exception;
import com.couchbase.utils.ClusterConnection;
import com.google.protobuf.ByteString;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;

import static com.couchbase.client.performer.core.util.TimeUtil.getTimeNow;


/**
 * SdkOperation performs each requested SDK operation
 */
public class JavaSdkCommandExecutor extends SdkCommandExecutor {
    private static final JsonTranscoder JSON_TRANSCODER = JsonTranscoder.create(DefaultJsonSerializer.create());
    private static final LegacyTranscoder LEGACY_TRANSCODER = LegacyTranscoder.create(DefaultJsonSerializer.create());

    private final ClusterConnection connection;

    public JavaSdkCommandExecutor(ClusterConnection connection, Counters counters) {
        super(counters);
        this.connection = connection;
    }

    @Override
    protected com.couchbase.client.protocol.run.Result performOperation(com.couchbase.client.protocol.sdk.Command op) {
        var result = com.couchbase.client.protocol.run.Result.newBuilder();

        if (op.hasInsert()){
            var request = op.getInsert();
            var collection = connection.collection(request.getLocation());
            var content = content(request.getContent());
            var docId = getDocId(request.getLocation());
            var options = createOptions(request);
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
            var options = createOptions(request);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            GetResult gr;
            if (options == null) gr = collection.get(docId);
            else gr = collection.get(docId, options);
            result.setElapsedNanos(System.nanoTime() - start);
            if (op.getReturnResult()) populateResult(result, gr);
            else setSuccess(result);
        } else if (op.hasRemove()){
            var request = op.getRemove();
            var collection = connection.collection(request.getLocation());
            var docId = getDocId(request.getLocation());
            var options = createOptions(request);
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
            var options = createOptions(request);
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
            var options = createOptions(request);
            var content = content(request.getContent());
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            MutationResult mr;
            if (options == null) mr = collection.upsert(docId, content);
            else mr = collection.upsert(docId, content, options);
            result.setElapsedNanos(System.nanoTime() - start);
            if (op.getReturnResult()) populateResult(result, mr);
            else setSuccess(result);
        } else {
            throw new UnsupportedOperationException(new IllegalArgumentException("Unknown operation"));
        }

        return result.build();
    }

    @Override
    protected Exception convertException(Throwable raw) {
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
                    out.setCause(convertException(raw.getCause()));
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

    private void setSuccess(com.couchbase.client.protocol.run.Result.Builder result) {
        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setSuccess(true));
    }

    private void populateResult(com.couchbase.client.protocol.run.Result.Builder result, MutationResult value) {
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

    private void populateResult(com.couchbase.client.protocol.run.Result.Builder result, GetResult value) {
        var builder = com.couchbase.client.protocol.sdk.kv.GetResult.newBuilder()
                .setCas(value.cas())
                // contentAsBytes was added later
                .setContent(ByteString.copyFrom(value.contentAs(JsonObject.class).toString().getBytes()));

        // [start:3.0.7]
        value.expiryTime().ifPresent(et -> builder.setExpiryTime(et.getEpochSecond()));
        // [end:3.0.7]

        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setGetResult(builder));
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

    private static @Nullable InsertOptions createOptions(com.couchbase.client.protocol.sdk.kv.Insert request) {
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
                    throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
                    // [end:<3.0.7]
                }
                else if (opts.getExpiry().hasRelativeSecs()) out.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                else throw new UnsupportedOperationException("Unknown expiry");
            }
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            return out;
        }
        else return null;
    }

    private static @Nullable RemoveOptions createOptions(com.couchbase.client.protocol.sdk.kv.Remove request) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = RemoveOptions.removeOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasDurability()) convertDurability(opts.getDurability(), out);
            if (opts.hasCas()) out.cas(opts.getCas());
            return out;
        }
        else return null;
    }

    private static @Nullable GetOptions createOptions(com.couchbase.client.protocol.sdk.kv.Get request) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = GetOptions.getOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasWithExpiry()) out.withExpiry(opts.getWithExpiry());
            if (opts.getProjectionCount() > 0) out.project(opts.getProjectionList().stream().toList());
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            return out;
        }
        else return null;
    }

    private static @Nullable ReplaceOptions createOptions(com.couchbase.client.protocol.sdk.kv.Replace request) {
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
                    throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
                    // [end:<3.0.7]
                }
                else if (opts.getExpiry().hasRelativeSecs()) out.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                else throw new UnsupportedOperationException("Unknown expiry");
            }
            if (opts.hasPreserveExpiry()) {
                // [start:3.1.5]
                out.preserveExpiry(opts.getPreserveExpiry());
                // [end:3.1.5]
                // [start:<3.1.5]
                throw new UnsupportedOperationException();
                // [end:<3.1.5]
            }
            if (opts.hasCas()) out.cas(opts.getCas());
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            return out;
        }
        else return null;
    }

    private static @Nullable UpsertOptions createOptions(com.couchbase.client.protocol.sdk.kv.Upsert request) {
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
                    throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
                    // [end:<3.0.7]
                }
                else if (opts.getExpiry().hasRelativeSecs()) out.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                else throw new UnsupportedOperationException("Unknown expiry");
            }
            if (opts.hasPreserveExpiry()) {
                // [start:3.1.5]
                out.preserveExpiry(opts.getPreserveExpiry());
                // [end:3.1.5]
                // [start:<3.1.5]
                throw new UnsupportedOperationException();
                // [end:<3.1.5]
            }
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            return out;
        }
        else return null;
    }
    
    private static void convertDurability(com.couchbase.client.protocol.shared.DurabilityType durability, CommonDurabilityOptions options) {
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
}
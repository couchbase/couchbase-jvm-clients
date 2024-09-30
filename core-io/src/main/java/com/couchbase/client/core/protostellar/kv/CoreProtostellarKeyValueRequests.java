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
package com.couchbase.client.core.protostellar.kv;

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.api.kv.CoreEncodedContent;
import com.couchbase.client.core.api.kv.CoreExpiry;
import com.couchbase.client.core.api.kv.CoreStoreSemantics;
import com.couchbase.client.core.api.kv.CoreSubdocGetCommand;
import com.couchbase.client.core.api.kv.CoreSubdocMutateCommand;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.google.protobuf.ByteString;
import com.couchbase.client.core.deps.com.google.protobuf.Timestamp;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.core.protostellar.ProtostellarKeyValueRequest;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.protostellar.kv.v1.CompressionEnabled;
import com.couchbase.client.protostellar.kv.v1.GetAndLockRequest;
import com.couchbase.client.protostellar.kv.v1.GetAndTouchRequest;
import com.couchbase.client.protostellar.kv.v1.GetRequest;
import com.couchbase.client.protostellar.kv.v1.InsertRequest;
import com.couchbase.client.protostellar.kv.v1.LookupInRequest;
import com.couchbase.client.protostellar.kv.v1.MutateInRequest;
import com.couchbase.client.protostellar.kv.v1.ReplaceRequest;
import com.couchbase.client.protostellar.kv.v1.TouchRequest;
import com.couchbase.client.protostellar.kv.v1.UnlockRequest;
import com.couchbase.client.protostellar.kv.v1.UpsertRequest;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateExistsParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateGetAndLockParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateGetAndTouchParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateGetParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateInsertParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateRemoveParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateReplaceParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateSubdocGetParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateSubdocMutateParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateTouchParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateUnlockParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateUpsertParams;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convert;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.toExpirySeconds;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.toExpiryTime;

/**
 * For creating Protostellar GRPC KV requests.
 */
@Stability.Internal
public class CoreProtostellarKeyValueRequests {
  private CoreProtostellarKeyValueRequests() {
  }

  static final Timestamp NO_EXPIRY = Timestamp.getDefaultInstance();

  public static ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> getRequest(CoreProtostellar core,
                                                                                                   CoreCommonOptions opts,
                                                                                                   CoreKeyspace keyspace,
                                                                                                   String key,
                                                                                                   List<String> projections,
                                                                                                   boolean withExpiry,
                                                                                                   CompressionConfig compressionConfig) {
    validateGetParams(opts, key, projections, withExpiry);

    GetRequest.Builder request = com.couchbase.client.protostellar.kv.v1.GetRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setKey(key);

    if (compressionConfig.enabled()) {
      request.setCompression(CompressionEnabled.COMPRESSION_ENABLED_OPTIONAL);
    }

    if (!projections.isEmpty()) {
      request.addAllProject(projections);
    }

    Duration timeout = CoreProtostellarUtil.kvTimeout(opts.timeout(), core);

    return new ProtostellarKeyValueRequest<>(request.build(),
      core,
      keyspace,
      key,
      CoreDurability.NONE,
      TracingIdentifiers.SPAN_REQUEST_KV_GET,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_GET, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      true,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      0);
  }

  public static ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetAndLockRequest> getAndLockRequest(CoreProtostellar core,
                                                                                                                 CoreCommonOptions opts,
                                                                                                                 CoreKeyspace keyspace,
                                                                                                                 String key,
                                                                                                                 Duration lockTime,
                                                                                                                 CompressionConfig compressionConfig) {

    validateGetAndLockParams(opts, key, lockTime);

    GetAndLockRequest.Builder request = com.couchbase.client.protostellar.kv.v1.GetAndLockRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setKey(key)
      .setLockTime((int) lockTime.toMillis());

    if (compressionConfig.enabled()) {
      request.setCompression(CompressionEnabled.COMPRESSION_ENABLED_OPTIONAL);
    }

    Duration timeout = CoreProtostellarUtil.kvTimeout(opts.timeout(), core);

    return new ProtostellarKeyValueRequest<>(request.build(),
      core,
      keyspace,
      key,
      CoreDurability.NONE,
      TracingIdentifiers.SPAN_REQUEST_KV_GET_AND_LOCK,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_GET_AND_LOCK, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      0);
  }

  public static ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetAndTouchRequest> getAndTouchRequest(CoreProtostellar core,
                                                                                                                   CoreCommonOptions opts,
                                                                                                                   CoreKeyspace keyspace,
                                                                                                                   String key,
                                                                                                                   CoreExpiry expiry,
                                                                                                                   CompressionConfig compressionConfig) {

    validateGetAndTouchParams(opts, key, expiry);

    GetAndTouchRequest.Builder request = com.couchbase.client.protostellar.kv.v1.GetAndTouchRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setKey(key);

    if (compressionConfig.enabled()) {
      request.setCompression(CompressionEnabled.COMPRESSION_ENABLED_OPTIONAL);
    }

    expiry.when(
      absolute -> request.setExpiryTime(toExpiryTime(absolute)),
      relative -> request.setExpirySecs(toExpirySeconds(relative)),
      () -> request.setExpiryTime(NO_EXPIRY)
    );

    Duration timeout = CoreProtostellarUtil.kvTimeout(opts.timeout(), core);

    return new ProtostellarKeyValueRequest<>(request.build(),
      core,
      keyspace,
      key,
      CoreDurability.NONE,
      TracingIdentifiers.SPAN_REQUEST_KV_GET_AND_TOUCH,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_GET_AND_TOUCH, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      0);
  }

  public static ProtostellarRequest<InsertRequest> insertRequest(CoreProtostellar core,
                                                                 CoreKeyspace keyspace,
                                                                 CoreCommonOptions opts,
                                                                 String key,
                                                                 Supplier<CoreEncodedContent> content,
                                                                 CoreDurability durability,
                                                                 CoreExpiry expiry,
                                                                 CompressionConfig compressionConfig) {
    validateInsertParams(opts, key, content, durability, expiry);

    RequestSpan span = createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_INSERT, durability, opts.parentSpan().orElse(null));

    ProtostellarCoreEncodedContent encoded = encodedContent(core, content, span, compressionConfig);

    InsertRequest.Builder request = InsertRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setKey(key)
      .setContentFlags(encoded.flags());

    if (encoded.compressed()) {
      request.setContentCompressed(encoded.bytes());
    }
    else {
      request.setContentUncompressed(encoded.bytes());
    }

    expiry.when(
      absolute -> request.setExpiryTime(toExpiryTime(absolute)),
      relative -> request.setExpirySecs(toExpirySeconds(relative)),
      () -> request.setExpiryTime(NO_EXPIRY)
    );

    if (!durability.isNone()) {
      request.setDurabilityLevel(convert(durability));
    }

    Duration timeout = CoreProtostellarUtil.kvDurableTimeout(opts.timeout(), durability, core);

    return new ProtostellarKeyValueRequest<>(request.build(),
      core,
      keyspace,
      key,
      durability,
      TracingIdentifiers.SPAN_REQUEST_KV_INSERT,
      span,
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      encoded.flags());
  }

  public static ProtostellarRequest<ReplaceRequest> replaceRequest(CoreProtostellar core,
                                                                   CoreKeyspace keyspace,
                                                                   CoreCommonOptions opts,
                                                                   String key,
                                                                   Supplier<CoreEncodedContent> content,
                                                                   long cas,
                                                                   CoreDurability durability,
                                                                   CoreExpiry expiry,
                                                                   boolean preserveExpiry,
                                                                   CompressionConfig compressionConfig) {
    validateReplaceParams(opts, key, content, cas, durability, expiry, preserveExpiry);

    RequestSpan span = createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_REPLACE, durability, opts.parentSpan().orElse(null));

    ProtostellarCoreEncodedContent encoded = encodedContent(core, content, span, compressionConfig);

    ReplaceRequest.Builder request = ReplaceRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setKey(key)
      .setContentFlags(encoded.flags());

    if (encoded.compressed()) {
      request.setContentCompressed(encoded.bytes());
    }
    else {
      request.setContentUncompressed(encoded.bytes());
    }

    if (cas != 0) {
      request.setCas(cas);
    }

    if (!preserveExpiry) {
      expiry.when(
        absolute -> request.setExpiryTime(toExpiryTime(absolute)),
        relative -> request.setExpirySecs(toExpirySeconds(relative)),
        () -> request.setExpiryTime(NO_EXPIRY)
      );
    }

    if (!durability.isNone()) {
      request.setDurabilityLevel(convert(durability));
    }

    Duration timeout = CoreProtostellarUtil.kvDurableTimeout(opts.timeout(), durability, core);

    return new ProtostellarKeyValueRequest<>(request.build(),
      core,
      keyspace,
      key,
      durability,
      TracingIdentifiers.SPAN_REQUEST_KV_REPLACE,
      span,
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      encoded.encodingTimeNanos());
  }

  public static ProtostellarRequest<UpsertRequest> upsertRequest(CoreProtostellar core,
                                                                 CoreKeyspace keyspace,
                                                                 CoreCommonOptions opts,
                                                                 String key,
                                                                 Supplier<CoreEncodedContent> content,
                                                                 CoreDurability durability,
                                                                 CoreExpiry expiry,
                                                                 boolean preserveExpiry,
                                                                 CompressionConfig compressionConfig) {
    validateUpsertParams(opts, key, content, durability, expiry, preserveExpiry);

    RequestSpan span = createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_UPSERT, durability, opts.parentSpan().orElse(null));

    ProtostellarCoreEncodedContent encoded = encodedContent(core, content, span, compressionConfig);

    UpsertRequest.Builder request = UpsertRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setKey(key)
      .setContentFlags(encoded.flags())
      .setPreserveExpiryOnExisting(preserveExpiry);

    if (encoded.compressed()) {
      request.setContentCompressed(encoded.bytes());
    }
    else {
      request.setContentUncompressed(encoded.bytes());
    }

    expiry.when(
      absolute -> request.setExpiryTime(toExpiryTime(absolute)),
      relative -> request.setExpirySecs(toExpirySeconds(relative)),
      () -> request.setExpiryTime(NO_EXPIRY)
    );

    if (!durability.isNone()) {
      request.setDurabilityLevel(convert(durability));
    }

    Duration timeout = CoreProtostellarUtil.kvDurableTimeout(opts.timeout(), durability, core);

    return new ProtostellarKeyValueRequest<>(request.build(),
      core,
      keyspace,
      key,
      durability,
      TracingIdentifiers.SPAN_REQUEST_KV_UPSERT,
      span,
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      encoded.encodingTimeNanos());
  }

  private static ProtostellarCoreEncodedContent encodedContent(CoreProtostellar core, Supplier<CoreEncodedContent> content, RequestSpan span, CompressionConfig compressionConfig) {
    RequestSpan encodeSpan = core.context().coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_ENCODING, span);
    long start = System.nanoTime();
    CoreEncodedContent encoded;
    boolean compressed = false;
    ByteString out = null;
    try {
      encoded = content.get();

      if (compressionConfig.enabled() && encoded.encoded().length >= compressionConfig.minSize()) {
        ByteBuf maybeCompressed = MemcacheProtocol.tryCompression(encoded.encoded(), compressionConfig.minRatio());
        if (maybeCompressed != null) {
          out = ByteString.copyFrom(maybeCompressed.array());
          compressed = true;
        }
      }

      if (out == null) {
        out = ByteString.copyFrom(encoded.encoded());
      }
    } finally {
      encodeSpan.end();
    }

    return new ProtostellarCoreEncodedContent(out, encoded.flags(), compressed, System.nanoTime() - start);
  }

  public static ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.RemoveRequest> removeRequest(CoreProtostellar core,
                                                                                                         CoreKeyspace keyspace,
                                                                                                         CoreCommonOptions opts,
                                                                                                         String key,
                                                                                                         long cas,
                                                                                                         CoreDurability durability) {
    validateRemoveParams(opts, key, cas, durability);

    com.couchbase.client.protostellar.kv.v1.RemoveRequest.Builder request = com.couchbase.client.protostellar.kv.v1.RemoveRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setKey(key);

    if (cas != 0) {
      request.setCas(cas);
    }

    if (!durability.isNone()) {
      request.setDurabilityLevel(convert(durability));
    }

    Duration timeout = CoreProtostellarUtil.kvDurableTimeout(opts.timeout(), durability, core);

    return new ProtostellarKeyValueRequest<>(request.build(),
      core,
      keyspace,
      key,
      durability,
      TracingIdentifiers.SPAN_REQUEST_KV_REMOVE,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_REMOVE, durability, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      0);
  }

  public static ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.ExistsRequest> existsRequest(CoreProtostellar core,
                                                                                                         CoreKeyspace keyspace,
                                                                                                         CoreCommonOptions opts,
                                                                                                         String key) {
    validateExistsParams(opts, key);

    com.couchbase.client.protostellar.kv.v1.ExistsRequest.Builder request = com.couchbase.client.protostellar.kv.v1.ExistsRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setKey(key);

    Duration timeout = CoreProtostellarUtil.kvTimeout(opts.timeout(), core);

    return new ProtostellarKeyValueRequest<>(request.build(),
      core,
      keyspace,
      key,
      CoreDurability.NONE,
      TracingIdentifiers.SPAN_REQUEST_KV_EXISTS,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_EXISTS, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      true,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      0);
  }

  public static ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.TouchRequest> touchRequest(CoreProtostellar core,
                                                                                                       CoreKeyspace keyspace,
                                                                                                       CoreCommonOptions opts,
                                                                                                       String key,
                                                                                                       CoreExpiry expiry) {
    validateTouchParams(opts, key, expiry);

    TouchRequest.Builder request = com.couchbase.client.protostellar.kv.v1.TouchRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setKey(key);

    expiry.when(
      absolute -> request.setExpiryTime(toExpiryTime(absolute)),
      relative -> request.setExpirySecs(toExpirySeconds(relative)),
      () -> request.setExpiryTime(NO_EXPIRY)
    );

    Duration timeout = CoreProtostellarUtil.kvTimeout(opts.timeout(), core);

    return new ProtostellarKeyValueRequest<>(request.build(),
      core,
      keyspace,
      key,
      CoreDurability.NONE,
      TracingIdentifiers.SPAN_REQUEST_KV_TOUCH,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_TOUCH, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      0);
  }

  public static ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.UnlockRequest> unlockRequest(CoreProtostellar core,
                                                                                                         CoreKeyspace keyspace,
                                                                                                         CoreCommonOptions opts,
                                                                                                         String key,
                                                                                                         long cas) {
    validateUnlockParams(opts, key, cas, keyspace.toCollectionIdentifier());

    UnlockRequest request = com.couchbase.client.protostellar.kv.v1.UnlockRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setCas(cas)
      .setKey(key)
      .build();

    Duration timeout = CoreProtostellarUtil.kvTimeout(opts.timeout(), core);

    return new ProtostellarKeyValueRequest<>(request,
      core,
      keyspace,
      key,
      CoreDurability.NONE,
      TracingIdentifiers.SPAN_REQUEST_KV_UNLOCK,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_UNLOCK, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      0);
  }

  public static ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.MutateInRequest> mutateInRequest(CoreProtostellar core,
                                                                                                             CoreKeyspace keyspace,
                                                                                                             CoreCommonOptions opts,
                                                                                                             String key,
                                                                                                             List<CoreSubdocMutateCommand> commands,
                                                                                                             CoreStoreSemantics storeSemantics,
                                                                                                             long cas,
                                                                                                             CoreDurability durability,
                                                                                                             CoreExpiry expiry,
                                                                                                             boolean preserveExpiry,
                                                                                                             boolean accessDeleted,
                                                                                                             boolean createAsDeleted) {
    validateSubdocMutateParams(opts, key, storeSemantics, cas);

    com.couchbase.client.protostellar.kv.v1.MutateInRequest.Builder request = com.couchbase.client.protostellar.kv.v1.MutateInRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setKey(key)
      .addAllSpecs(commands.stream()
        .map(command -> {
          MutateInRequest.Spec.Operation operation;
          String path = command.path();

          switch (command.type()) {
            case COUNTER:
              operation = MutateInRequest.Spec.Operation.OPERATION_COUNTER;
              break;
            case REPLACE:
              operation = MutateInRequest.Spec.Operation.OPERATION_REPLACE;
              break;
            case DICT_ADD:
              operation = MutateInRequest.Spec.Operation.OPERATION_INSERT;
              break;
            case DICT_UPSERT:
              operation = MutateInRequest.Spec.Operation.OPERATION_UPSERT;
              break;
            case ARRAY_PUSH_FIRST:
              operation = MutateInRequest.Spec.Operation.OPERATION_ARRAY_PREPEND;
              break;
            case ARRAY_PUSH_LAST:
              operation = MutateInRequest.Spec.Operation.OPERATION_ARRAY_APPEND;
              break;
            case ARRAY_ADD_UNIQUE:
              operation = MutateInRequest.Spec.Operation.OPERATION_ARRAY_ADD_UNIQUE;
              break;
            case ARRAY_INSERT:
              operation = MutateInRequest.Spec.Operation.OPERATION_ARRAY_INSERT;
              break;
            case DELETE:
              operation = MutateInRequest.Spec.Operation.OPERATION_REMOVE;
              break;
            case SET_DOC:
              operation = MutateInRequest.Spec.Operation.OPERATION_REPLACE;
              path = "";
              break;
            case DELETE_DOC:
              operation = MutateInRequest.Spec.Operation.OPERATION_REMOVE;
              path = "";
              break;
            default:
              throw new IllegalArgumentException("Sub-Document mutateIn command " + command.type() + " is not supported in Protostellar");
          }

          MutateInRequest.Spec.Builder builder = MutateInRequest.Spec.newBuilder()
            .setOperation(operation)
            .setPath(path)
            .setContent(ByteString.copyFrom(command.fragment()));

          if (command.xattr() || command.expandMacro() || command.createParent()) {
            MutateInRequest.Spec.Flags.Builder flagsBuilder = MutateInRequest.Spec.Flags.newBuilder();

            if (command.xattr()) {
              flagsBuilder.setXattr(command.xattr());
            }

            if (command.createParent()) {
              flagsBuilder.setCreatePath(command.createParent());
            }

            if (command.expandMacro()) {
              throw new IllegalArgumentException("expandMacro is not supported in Protostellar");
            }

            builder.setFlags(flagsBuilder);
          }

          return builder.build();
        })
        .collect(Collectors.toList()));

    switch (storeSemantics) {
      case REPLACE:
        request.setStoreSemantic(MutateInRequest.StoreSemantic.STORE_SEMANTIC_REPLACE);
        break;
      case UPSERT:
        request.setStoreSemantic(MutateInRequest.StoreSemantic.STORE_SEMANTIC_UPSERT);
        break;
      case INSERT:
        request.setStoreSemantic(MutateInRequest.StoreSemantic.STORE_SEMANTIC_INSERT);
        break;
      default:
        throw new IllegalArgumentException("Sub-Document store semantic " + storeSemantics + " is not supported in Protostellar");
    }

    if (cas != 0) {
      request.setCas(cas);
    }

    if (accessDeleted) {
      request.setFlags(MutateInRequest.Flags.newBuilder()
        .setAccessDeleted(accessDeleted));
    }

    if (!durability.isNone()) {
      request.setDurabilityLevel(convert(durability));
    }

    if (createAsDeleted) {
      throw new IllegalArgumentException("createAsDeleted is not supported in mutateIn in Protostellar");
    }

    if (!preserveExpiry) {
      expiry.when(
        absolute -> request.setExpiryTime(toExpiryTime(absolute)),
        relative -> request.setExpirySecs(toExpirySeconds(relative)),
        () -> request.setExpiryTime(NO_EXPIRY)
      );
    }

    Duration timeout = CoreProtostellarUtil.kvDurableTimeout(opts.timeout(), durability, core);
    return new ProtostellarKeyValueRequest<>(request.build(),
      core,
      keyspace,
      key,
      durability,
      TracingIdentifiers.SPAN_REQUEST_KV_MUTATE_IN,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_MUTATE_IN, durability, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      0);
  }

  public static ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.LookupInRequest> lookupInRequest(CoreProtostellar core,
                                                                                                             CoreKeyspace keyspace,
                                                                                                             CoreCommonOptions opts,
                                                                                                             String key,
                                                                                                             List<CoreSubdocGetCommand> commands,
                                                                                                             boolean accessDeleted) {
    validateSubdocGetParams(opts, key, commands);

    com.couchbase.client.protostellar.kv.v1.LookupInRequest.Builder request = com.couchbase.client.protostellar.kv.v1.LookupInRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setKey(key)
      .addAllSpecs(commands.stream()
        .map(command -> {
          LookupInRequest.Spec.Operation operation;
          String path = command.path();

          switch (command.type()) {
            case GET:
              operation = LookupInRequest.Spec.Operation.OPERATION_GET;
              break;
            case EXISTS:
              operation = LookupInRequest.Spec.Operation.OPERATION_EXISTS;
              break;
            case COUNT:
              operation = LookupInRequest.Spec.Operation.OPERATION_COUNT;
              break;
            case GET_DOC:
              operation = LookupInRequest.Spec.Operation.OPERATION_GET;
              path = "";
              break;
            default:
              throw new IllegalArgumentException("Sub-Document lookupIn command " + command.type() + " is not supported in Protostellar");
          }

          LookupInRequest.Spec.Builder builder = LookupInRequest.Spec.newBuilder()
            .setOperation(operation)
            .setPath(path);

          if (command.xattr()) {
            LookupInRequest.Spec.Flags.Builder flagsBuilder = LookupInRequest.Spec.Flags.newBuilder();

            if (command.xattr()) {
              flagsBuilder.setXattr(command.xattr());
            }

            builder.setFlags(flagsBuilder);
          }

          return builder.build();
        })
        .collect(Collectors.toList()));

    if (accessDeleted) {
      request.setFlags(LookupInRequest.Flags.newBuilder()
        .setAccessDeleted(accessDeleted));
    }

    Duration timeout = CoreProtostellarUtil.kvTimeout(opts.timeout(), core);
    return new ProtostellarKeyValueRequest<>(request.build(),
      core,
      keyspace,
      key,
      CoreDurability.NONE,
      TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      0);
  }
}

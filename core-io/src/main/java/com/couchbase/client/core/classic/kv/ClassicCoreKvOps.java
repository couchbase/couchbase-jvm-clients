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

package com.couchbase.client.core.classic.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.api.kv.CoreEncodedContent;
import com.couchbase.client.core.api.kv.CoreExistsResult;
import com.couchbase.client.core.api.kv.CoreExpiry;
import com.couchbase.client.core.api.kv.CoreGetResult;
import com.couchbase.client.core.api.kv.CoreKvOps;
import com.couchbase.client.core.api.kv.CoreKvResponseMetadata;
import com.couchbase.client.core.api.kv.CoreLookupInMacro;
import com.couchbase.client.core.api.kv.CoreMutationResult;
import com.couchbase.client.core.api.kv.CoreStoreSemantics;
import com.couchbase.client.core.api.kv.CoreSubdocGetCommand;
import com.couchbase.client.core.api.kv.CoreSubdocGetResult;
import com.couchbase.client.core.api.kv.CoreSubdocMutateCommand;
import com.couchbase.client.core.api.kv.CoreSubdocMutateResult;
import com.couchbase.client.core.api.kv.CoreReadPreference;
import com.couchbase.client.core.classic.ClassicHelper;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.DocumentUnretrievableException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.kv.CoreRangeScan;
import com.couchbase.client.core.kv.CoreRangeScanItem;
import com.couchbase.client.core.kv.CoreSamplingScan;
import com.couchbase.client.core.kv.CoreScanOptions;
import com.couchbase.client.core.kv.CoreScanType;
import com.couchbase.client.core.kv.RangeScanOrchestrator;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.kv.CodecFlags;
import com.couchbase.client.core.msg.kv.GetAndLockRequest;
import com.couchbase.client.core.msg.kv.GetAndTouchRequest;
import com.couchbase.client.core.msg.kv.GetMetaRequest;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.kv.RemoveRequest;
import com.couchbase.client.core.msg.kv.ReplaceRequest;
import com.couchbase.client.core.msg.kv.SubDocumentField;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocGetResponse;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.core.msg.kv.TouchRequest;
import com.couchbase.client.core.msg.kv.UnlockRequest;
import com.couchbase.client.core.msg.kv.UpsertRequest;
import com.couchbase.client.core.projections.ProjectionsApplier;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.kv.ReplicaHelper;
import com.couchbase.client.core.util.BucketConfigUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateExistsParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateGetAllReplicasParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateGetAndLockParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateGetAndTouchParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateGetAnyReplicaParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateGetParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateInsertParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateRemoveParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateReplaceParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateSubdocGetAllParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateSubdocGetAnyParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateSubdocGetParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateSubdocMutateParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateTouchParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateUnlockParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateUpsertParams;
import static com.couchbase.client.core.api.kv.CoreStoreSemantics.INSERT;
import static com.couchbase.client.core.api.kv.CoreStoreSemantics.REVIVE;
import static com.couchbase.client.core.classic.ClassicExpiryHelper.encode;
import static com.couchbase.client.core.classic.ClassicHelper.maybeWrapWithLegacyDurability;
import static com.couchbase.client.core.classic.ClassicHelper.setClientContext;
import static com.couchbase.client.core.error.DefaultErrorUtil.keyValueStatusToException;
import static com.couchbase.client.core.msg.ResponseStatus.EXISTS;
import static com.couchbase.client.core.msg.ResponseStatus.LOCKED;
import static com.couchbase.client.core.msg.ResponseStatus.NOT_FOUND;
import static com.couchbase.client.core.msg.ResponseStatus.NOT_STORED;
import static com.couchbase.client.core.msg.ResponseStatus.SUBDOC_FAILURE;
import static com.couchbase.client.core.util.Validators.notNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public final class ClassicCoreKvOps implements CoreKvOps {
  private final Core core;
  private final CoreContext ctx;
  private final Duration defaultKvTimeout;
  private final Duration defaultKvDurableTimeout;
  private final RetryStrategy defaultRetryStrategy;
  private final CollectionIdentifier collectionIdentifier;
  private final CoreKeyspace keyspace;
  private final RequestTracer requestTracer;
  private final RangeScanOrchestrator rangeScanOrchestrator;

  public ClassicCoreKvOps(Core core, CoreKeyspace keyspace) {
    this.core = requireNonNull(core);
    this.ctx = core.context();
    this.defaultKvTimeout = ctx.environment().timeoutConfig().kvTimeout();
    this.defaultKvDurableTimeout = ctx.environment().timeoutConfig().kvDurableTimeout();
    this.defaultRetryStrategy = ctx.environment().retryStrategy();
    this.requestTracer = ctx.coreResources().requestTracer();
    this.keyspace = requireNonNull(keyspace);
    this.collectionIdentifier = keyspace.toCollectionIdentifier();
    this.rangeScanOrchestrator = new RangeScanOrchestrator(core, collectionIdentifier);
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAsync(CoreCommonOptions common, String key, List<String> projections, boolean withExpiry) {
    validateGetParams(common, key, projections, withExpiry);

    Duration timeout = timeout(common);
    RetryStrategy retryStrategy = retryStrategy(common);

    if (!withExpiry && projections.isEmpty()) {
      RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_GET);
      GetRequest request = new GetRequest(key, timeout, ctx, collectionIdentifier, retryStrategy, span);
      setClientContext(request, common);

      return newAsyncResponse(
          request,
          it -> new CoreGetResult(
              CoreKvResponseMetadata.from(it.flexibleExtras()),
              keyspace,
              key,
              it.content(),
              it.flags(),
              it.cas(),
              null,
              false
          )
      );
    }

    SubdocGetRequest request = getWithProjectionsOrExpiryRequest(common, key, projections, withExpiry);
    return newAsyncResponse(
        request,
        (req, res) -> {
          if (res.status() != SUBDOC_FAILURE) {
            throw keyValueStatusToException(request, res);
          }
        },
        it -> parseGetWithProjectionsOrExpiry(key, it)
    );
  }

  private SubdocGetRequest getWithProjectionsOrExpiryRequest(
      CoreCommonOptions common,
      String key,
      List<String> projections,
      boolean withExpiry
  ) {
    validateGetParams(common, key, projections, withExpiry);
    checkProjectionLimits(projections, withExpiry);

    Duration timeout = timeout(common);
    RetryStrategy retryStrategy = retryStrategy(common);
    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN);
    List<SubdocGetRequest.Command> commands = new ArrayList<>(16);

    if (!projections.isEmpty()) {
      for (String projection : projections) {
        commands.add(new SubdocGetRequest.Command(SubdocCommandType.GET, projection, false, commands.size()));
      }
    } else {
      commands.add(new SubdocGetRequest.Command(SubdocCommandType.GET_DOC, "", false, commands.size()));
    }

    if (withExpiry) {
      // xattrs must go first
      commands.add(0, new SubdocGetRequest.Command(SubdocCommandType.GET, CoreLookupInMacro.EXPIRY_TIME, true, commands.size()));

      // If we have projections, there is no need to fetch the flags
      // since only JSON is supported that implies the flags.
      // This will also "force" the transcoder on the read side to be
      // JSON aware since the flags are going to be hard-set to the
      // JSON compat flags.
      if (projections.isEmpty()) {
        commands.add(1, new SubdocGetRequest.Command(SubdocCommandType.GET, CoreLookupInMacro.FLAGS, true, commands.size()));
      }
    }

    return new SubdocGetRequest(
        timeout,
        ctx,
        collectionIdentifier,
        retryStrategy,
        key,
        (byte) 0x00,
        commands,
        span
    );
  }

  private CoreGetResult parseGetWithProjectionsOrExpiry(String key, SubdocGetResponse response) {
    if (response.error().isPresent()) {
      throw response.error().get();
    }

    // Simulate logic that used to be present in SubdocGetResponse.
    if (response.values().length == 1 && response.values()[0].error().isPresent()) {
      throw response.values()[0].error().get();
    }

    long cas = response.cas();

    byte[] exptime = null;
    byte[] content = null;
    byte[] flags = null;

    for (SubDocumentField value : response.values()) {
      if (value != null) {
        if (CoreLookupInMacro.EXPIRY_TIME.equals(value.path())) {
          exptime = value.value();
        } else if (CoreLookupInMacro.FLAGS.equals(value.path())) {
          flags = value.value();
        } else if (value.path().isEmpty()) {
          content = value.value();
        }
      }
    }

    int convertedFlags = flags == null || flags.length == 0
        ? CodecFlags.JSON_COMPAT_FLAGS
        : Integer.parseInt(new String(flags, UTF_8));

    if (content == null) {
      try {
        content = ProjectionsApplier.reconstructDocument(response);
      } catch (Exception e) {
        throw new CouchbaseException("Unexpected Exception while decoding Sub-Document get", e);
      }
    }

    Optional<Instant> expiration = Optional.empty();
    if (exptime != null && exptime.length > 0) {
      long parsed = Long.parseLong(new String(exptime, UTF_8));
      if (parsed > 0) {
        expiration = Optional.of(Instant.ofEpochSecond(parsed));
      }
    }

    return new CoreGetResult(
        CoreKvResponseMetadata.from(response.flexibleExtras()),
        keyspace,
        key,
        content,
        convertedFlags,
        cas,
        expiration.orElse(null),
        false
    );
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAndLockAsync(
      CoreCommonOptions common,
      String key,
      Duration lockTime
  ) {
    validateGetAndLockParams(common, key, lockTime);

    Duration timeout = timeout(common);
    RetryStrategy retryStrategy = retryStrategy(common);
    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_GET_AND_LOCK);

    GetAndLockRequest request = new GetAndLockRequest(key, timeout, ctx, collectionIdentifier, retryStrategy, lockTime, span);
    setClientContext(request, common);

    return newAsyncResponse(
        request,
        it -> new CoreGetResult(
            CoreKvResponseMetadata.from(it.flexibleExtras()),
            keyspace,
            key,
            it.content(),
            it.flags(),
            it.cas(),
            null,
            false
        )
    );
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAndTouchAsync(
      CoreCommonOptions common,
      String key,
      CoreExpiry expiry
  ) {
    validateGetAndTouchParams(common, key, expiry);

    Duration timeout = timeout(common);
    RetryStrategy retryStrategy = retryStrategy(common);
    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_GET_AND_TOUCH);

    GetAndTouchRequest request = new GetAndTouchRequest(key, timeout, ctx, collectionIdentifier, retryStrategy, encode(expiry), span);
    setClientContext(request, common);

    return newAsyncResponse(
        request,
        it -> new CoreGetResult(
            CoreKvResponseMetadata.from(it.flexibleExtras()),
            keyspace,
            key,
            it.content(),
            it.flags(),
            it.cas(),
            null,
            false
        )
    );
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> insertAsync(
      CoreCommonOptions common,
      String key,
      Supplier<CoreEncodedContent> content,
      CoreDurability durability,
      CoreExpiry expiry
  ) {
    validateInsertParams(common, key, content, durability, expiry);

    Duration timeout = timeout(common, durability);
    RetryStrategy retryStrategy = retryStrategy(common);

    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_INSERT);
    RequestSpan encodingSpan = span(span, TracingIdentifiers.SPAN_REQUEST_ENCODING);

    long encodingStartNanos = System.nanoTime();
    CoreEncodedContent coreContent;
    try {
      coreContent = content.get();
    } finally {
      encodingSpan.end();
    }
    long encodingNanos = System.nanoTime() - encodingStartNanos;

    InsertRequest request = new InsertRequest(
        key,
        coreContent.encoded(),
        encode(expiry),
        coreContent.flags(),
        timeout,
        ctx,
        collectionIdentifier,
        retryStrategy,
        durability.levelIfSynchronous(),
        span
    );

    request.context()
        .clientContext(common.clientContext())
        .encodeLatency(encodingNanos);

    CompletableFuture<CoreMutationResult> future = executeWithoutMarkingComplete(
        request,
        (req, res) -> {
          if (res.status() == EXISTS || res.status() == NOT_STORED) {
            throw new DocumentExistsException(KeyValueErrorContext.completedRequest(req, res));
          }
          throw res.errorIfNeeded(request);
        },
        it -> new CoreMutationResult(
            CoreKvResponseMetadata.from(it.flexibleExtras()),
            keyspace,
            key,
            it.cas(),
            it.mutationToken()
        )
    );

    future = maybeWrapWithLegacyDurability(future, key, durability, core, request)
        .whenComplete((response, failure) -> markComplete(request, failure));

    return ClassicHelper.newAsyncResponse(request, future);
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> upsertAsync(
      CoreCommonOptions common,
      String key,
      Supplier<CoreEncodedContent> content,
      CoreDurability durability,
      CoreExpiry expiry,
      boolean preserveExpiry
  ) {
    validateUpsertParams(common, key, content, durability, expiry, preserveExpiry);

    Duration timeout = timeout(common, durability);
    RetryStrategy retryStrategy = retryStrategy(common);

    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_UPSERT);
    RequestSpan encodingSpan = span(span, TracingIdentifiers.SPAN_REQUEST_ENCODING);

    long encodingStartNanos = System.nanoTime();
    CoreEncodedContent coreContent;
    try {
      coreContent = content.get();
    } finally {
      encodingSpan.end();
    }
    long encodingNanos = System.nanoTime() - encodingStartNanos;

    UpsertRequest request = new UpsertRequest(
        key,
        coreContent.encoded(),
        encode(expiry),
        preserveExpiry,
        coreContent.flags(),
        timeout,
        ctx,
        collectionIdentifier,
        retryStrategy,
        durability.levelIfSynchronous(),
        span
    );

    request.context()
        .clientContext(common.clientContext())
        .encodeLatency(encodingNanos);

    CompletableFuture<CoreMutationResult> future = executeWithoutMarkingComplete(
        request,
        it -> new CoreMutationResult(
            CoreKvResponseMetadata.from(it.flexibleExtras()),
            keyspace,
            key,
            it.cas(),
            it.mutationToken()
        )
    );

    future = maybeWrapWithLegacyDurability(future, key, durability, core, request)
        .whenComplete((response, failure) -> markComplete(request, failure));

    return ClassicHelper.newAsyncResponse(request, future);
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> replaceAsync(
      CoreCommonOptions common,
      String key,
      Supplier<CoreEncodedContent> content,
      long cas,
      CoreDurability durability,
      CoreExpiry expiry,
      boolean preserveExpiry
  ) {
    validateReplaceParams(common, key, content, cas, durability, expiry, preserveExpiry);

    Duration timeout = timeout(common, durability);
    RetryStrategy retryStrategy = retryStrategy(common);

    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_REPLACE);
    RequestSpan encodingSpan = span(span, TracingIdentifiers.SPAN_REQUEST_ENCODING);

    long encodingStartNanos = System.nanoTime();
    CoreEncodedContent coreContent;
    try {
      coreContent = content.get();
    } finally {
      encodingSpan.end();
    }
    long encodingNanos = System.nanoTime() - encodingStartNanos;

    ReplaceRequest request = new ReplaceRequest(
        key,
        coreContent.encoded(),
        encode(expiry),
        preserveExpiry,
        coreContent.flags(),
        timeout,
        cas,
        ctx,
        collectionIdentifier,
        retryStrategy,
        durability.levelIfSynchronous(),
        span
    );

    request.context()
        .clientContext(common.clientContext())
        .encodeLatency(encodingNanos);

    CompletableFuture<CoreMutationResult> future = executeWithoutMarkingComplete(
        request,
        it -> new CoreMutationResult(
            CoreKvResponseMetadata.from(it.flexibleExtras()),
            keyspace,
            key,
            it.cas(),
            it.mutationToken()
        )
    );

    future = maybeWrapWithLegacyDurability(future, key, durability, core, request)
        .whenComplete((response, failure) -> markComplete(request, failure));

    return ClassicHelper.newAsyncResponse(request, future);
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> removeAsync(
      CoreCommonOptions common,
      String key,
      long cas,
      CoreDurability durability
  ) {
    validateRemoveParams(common, key, cas, durability);

    Duration timeout = timeout(common, durability);
    RetryStrategy retryStrategy = retryStrategy(common);

    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_REMOVE);

    RemoveRequest request = new RemoveRequest(
        key,
        cas,
        timeout,
        ctx,
        collectionIdentifier,
        retryStrategy,
        durability.levelIfSynchronous(),
        span
    );

    request.context()
        .clientContext(common.clientContext());

    CompletableFuture<CoreMutationResult> future = executeWithoutMarkingComplete(
        request,
        it -> new CoreMutationResult(
            CoreKvResponseMetadata.from(it.flexibleExtras()),
            keyspace,
            key,
            it.cas(),
            it.mutationToken()
        )
    );

    future = maybeWrapWithLegacyDurability(future, key, durability, core, request)
        .whenComplete((response, failure) -> markComplete(request, failure));

    return ClassicHelper.newAsyncResponse(request, future);
  }

  @Override
  public CoreAsyncResponse<CoreExistsResult> existsAsync(CoreCommonOptions common, String key) {
    validateExistsParams(common, key);

    Duration timeout = timeout(common);
    RetryStrategy retryStrategy = retryStrategy(common);
    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_EXISTS);

    GetMetaRequest request = new GetMetaRequest(key, timeout, ctx, collectionIdentifier, retryStrategy, span);
    setClientContext(request, common);

    return newAsyncResponse(
        request,
        (req, res) -> {
          if (res.status() != NOT_FOUND) {
            throw keyValueStatusToException(req, res);
          }
        },
        it -> new CoreExistsResult(
            CoreKvResponseMetadata.from(it.flexibleExtras()),
            keyspace,
            key,
            it.cas(),
            it.status().success() && !it.deleted() // exists?
        )
    );
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> touchAsync(CoreCommonOptions common, String key, CoreExpiry expiry) {
    validateTouchParams(common, key, expiry);

    Duration timeout = timeout(common);
    RetryStrategy retryStrategy = retryStrategy(common);
    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_TOUCH);

    TouchRequest request = new TouchRequest(timeout, ctx, collectionIdentifier, retryStrategy, key, encode(expiry), span);
    setClientContext(request, common);

    return newAsyncResponse(
        request,
        it -> new CoreMutationResult(
            CoreKvResponseMetadata.from(it.flexibleExtras()),
            keyspace,
            key,
            it.cas(),
            it.mutationToken()
        )
    );
  }

  @Override
  public CoreAsyncResponse<Void> unlockAsync(CoreCommonOptions common, String key, long cas) {
    validateUnlockParams(common, key, cas, collectionIdentifier);

    Duration timeout = timeout(common);
    RetryStrategy retryStrategy = retryStrategy(common);
    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_UNLOCK);

    UnlockRequest request = new UnlockRequest(timeout, ctx, collectionIdentifier, retryStrategy, key, cas, span);
    setClientContext(request, common);

    return newAsyncResponse(
        request,
        (req, res) -> {
          if (res.status() == LOCKED) {
            throw new CasMismatchException(KeyValueErrorContext.completedRequest(req, res));
          }
          throw keyValueStatusToException(req, res);
        },
        it -> null
    );
  }

  @Override
  public CoreAsyncResponse<CoreSubdocGetResult> subdocGetAsync(
      CoreCommonOptions common,
      String key,
      List<CoreSubdocGetCommand> commands,
      boolean accessDeleted
  ) {
    validateSubdocGetParams(common, key, commands);

    Duration timeout = timeout(common);
    RetryStrategy retryStrategy = retryStrategy(common);
    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN);

    byte flags = accessDeleted ? SubdocMutateRequest.SUBDOC_DOC_FLAG_ACCESS_DELETED : 0;

    SubdocGetRequest request = SubdocGetRequest.create(
        timeout,
        ctx,
        collectionIdentifier,
        retryStrategy,
        key,
        flags,
        commands,
        span
    );
    request.context()
        .clientContext(common.clientContext());

    return newAsyncResponse(
        request,
        (req, res) -> {
          // This is a top-level exception meant to be thrown from the lookupIn call.
          if (res.error().isPresent()) {
            throw res.error().get();
          }

          if (res.status() == SUBDOC_FAILURE) {
            // Ignore. The failure of any one lookup command does not cause the whole request to fail.
            return;
          }

          // This should be superfluous now - if the op failed then error() should be set - but leaving as a fail-safe.
          commonKvResponseCheck(req, res);
        },
        it -> it.toCore(keyspace, key)
    );
  }

  @Override
  public Flux<CoreGetResult> getAllReplicasReactive(CoreCommonOptions common, String key, CoreReadPreference readPreference) {
    validateGetAllReplicasParams(common, key);

    Duration timeout = timeout(common);
    RetryStrategy retryStrategy = retryStrategy(common);

    return ReplicaHelper.getAllReplicasReactive(
        core,
        collectionIdentifier,
        key,
        timeout,
        retryStrategy,
        common.clientContext(),
        common.parentSpan().orElse(null),
        readPreference
    ).map(it -> new CoreGetResult(
        CoreKvResponseMetadata.from(it.getResponse().flexibleExtras()),
        keyspace,
        key,
        it.getResponse().content(),
        it.getResponse().flags(),
        it.getResponse().cas(),
        null,
        it.isFromReplica()
    ));
  }

  @Override
  public Mono<CoreGetResult> getAnyReplicaReactive(CoreCommonOptions common, String key, CoreReadPreference readPreference) {
    validateGetAnyReplicaParams(common, key);

    RequestSpan getAnySpan = span(common, TracingIdentifiers.SPAN_GET_ANY_REPLICA);
    return getAllReplicasReactive(common.withParentSpan(getAnySpan), key, readPreference)
        .next()
        .doFinally(signalType -> getAnySpan.end());
  }

  @Override
  public Flux<CoreSubdocGetResult> subdocGetAllReplicasReactive(CoreCommonOptions common, String key, List<CoreSubdocGetCommand> commands, CoreReadPreference readPreference) {
    validateSubdocGetAllParams(common, key, commands);

    Duration timeout = timeout(common);
    RetryStrategy retryStrategy = retryStrategy(common);

    return ReplicaHelper.lookupInAllReplicasReactive(
        core,
        collectionIdentifier,
        key,
        commands,
        timeout,
        retryStrategy,
        common.clientContext(),
        common.parentSpan().orElse(null),
        readPreference
    );
  }

  @Override
  public Mono<CoreSubdocGetResult> subdocGetAnyReplicaReactive(CoreCommonOptions common, String key, List<CoreSubdocGetCommand> commands, CoreReadPreference readPreference) {
    validateSubdocGetAnyParams(common, key, commands);
    RequestSpan getAnySpan = span(common, TracingIdentifiers.SPAN_GET_ANY_REPLICA);
    return subdocGetAllReplicasReactive(common.withParentSpan(getAnySpan), key, commands, readPreference)
        .next()
        .switchIfEmpty(Mono.error(new DocumentUnretrievableException(ReducedKeyValueErrorContext.create(key, collectionIdentifier))))
        .doFinally(signalType -> getAnySpan.end());
  }

  @Override
  public CoreAsyncResponse<CoreSubdocMutateResult> subdocMutateAsync(
      CoreCommonOptions common,
      String key,
      Supplier<List<CoreSubdocMutateCommand>> commands,
      CoreStoreSemantics storeSemantics,
      long cas,
      CoreDurability durability,
      CoreExpiry expiry,
      boolean preserveExpiry,
      boolean accessDeleted,
      boolean createAsDeleted
  ) {
    validateSubdocMutateParams(common, key, storeSemantics, cas);
    Duration timeout = timeout(common, durability);
    RetryStrategy retryStrategy = retryStrategy(common);

    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_MUTATE_IN);

    final RequestSpan encodeSpan = span(span, TracingIdentifiers.SPAN_REQUEST_ENCODING);
    long encodingStartNanos = System.nanoTime();
    List<CoreSubdocMutateCommand> encodedCommands;
    try {
      encodedCommands = commands.get();
    } finally {
      encodeSpan.end();
    }
    long encodingEndNanos = System.nanoTime();

    if (encodedCommands.isEmpty()) {
      throw SubdocMutateRequest.errIfNoCommands(ReducedKeyValueErrorContext.create(key, collectionIdentifier));
    } else if (encodedCommands.size() > SubdocMutateRequest.SUBDOC_MAX_FIELDS) {
      throw SubdocMutateRequest.errIfTooManyCommands(ReducedKeyValueErrorContext.create(key, collectionIdentifier));
    }

    // Do a pre-flight check to ensure the server supports these features.
    // It's unclear if this is required, but it's what the previous
    // implementation did, and we're erring on the side of caution.
    // The simper alternative is to let the operation fail, then translate the ambiguous
    // error code into a FeatureNotAvailableException. (Revert this commit to see
    // what that would look like.)
    boolean needsBucketConfig = createAsDeleted || storeSemantics == REVIVE;
    CompletableFuture<BucketConfig> bucketConfigFuture = needsBucketConfig
        ? BucketConfigUtil.waitForBucketConfig(core, keyspace.bucket(), timeout).toFuture()
        : CompletableFuture.completedFuture(null);

    AtomicReference<SubdocMutateRequest> requestHolder = new AtomicReference<>();

    CompletableFuture<CoreSubdocMutateResult> finalResultFuture = bucketConfigFuture.thenCompose(bucketConfig -> {

      SubdocMutateRequest request = new SubdocMutateRequest(
          timeout, ctx, collectionIdentifier, bucketConfig, retryStrategy, key,
          storeSemantics,
          accessDeleted, createAsDeleted, encodedCommands, encode(expiry), preserveExpiry, cas,
          durability.levelIfSynchronous(), span
      );

      request.context()
          .clientContext(common.clientContext())
          .encodeLatency(encodingEndNanos - encodingStartNanos);

      requestHolder.set(request);

      CompletableFuture<CoreSubdocMutateResult> subdocRequestFuture = executeWithoutMarkingComplete(
          request,
          (req, res) -> {
            throw res.throwError(request, storeSemantics == INSERT);
          },
          it -> new CoreSubdocMutateResult(
              keyspace,
              key,
              CoreKvResponseMetadata.from(it.flexibleExtras()),
              it.cas(),
              it.mutationToken(),
              Arrays.asList(it.values())
          )
      );

      return maybeWrapWithLegacyDurability(subdocRequestFuture, key, durability, core, request)
          .whenComplete((response, failure) -> markComplete(request, failure));
    });

    return new CoreAsyncResponse<>(
        finalResultFuture,
        () -> Optional.ofNullable(requestHolder.get())
            .ifPresent(it -> it.cancel(CancellationReason.STOPPED_LISTENING))
    );
  }

  @Override
  public Flux<CoreRangeScanItem> scanRequestReactive(final CoreScanType scanType, final CoreScanOptions options) {
    Flux<CoreRangeScanItem> coreScanStream;

    if (scanType instanceof CoreRangeScan) {
      coreScanStream = rangeScanOrchestrator.rangeScan((CoreRangeScan) scanType, options);
    } else if (scanType instanceof CoreSamplingScan) {
      coreScanStream = rangeScanOrchestrator.samplingScan((CoreSamplingScan) scanType,options);
    } else {
      return Flux.error(InvalidArgumentException.fromMessage("Unsupported ScanType: " + scanType));
    }

    if (options.idsOnly()) {
      return coreScanStream.map(item -> CoreRangeScanItem.keyOnly(item.keyBytes()));
    } else {
      return coreScanStream.map(item -> CoreRangeScanItem.keyAndBody(item.flags(), item.expiry(), item.seqno(),
          item.cas(), item.keyBytes(), item.value()));
    }
  }

  private <T extends BaseResponse, R> CompletableFuture<R> execute(
      KeyValueRequest<T> request,
      Function<T, R> responseTransformer
  ) {
    return execute(request, ClassicCoreKvOps::commonKvResponseCheck, responseTransformer);
  }

  private <T extends BaseResponse, R> CompletableFuture<R> execute(
      KeyValueRequest<T> request,
      BiConsumer<KeyValueRequest<T>, T> responseChecker,
      Function<T, R> responseTransformer
  ) {
    return executeWithoutMarkingComplete(request, responseChecker, responseTransformer)
        .whenComplete((response, failure) -> markComplete(request, failure));
  }

  private <T extends BaseResponse, R> CompletableFuture<R> executeWithoutMarkingComplete(
      KeyValueRequest<T> request,
      Function<T, R> responseTransformer
  ) {
    return executeWithoutMarkingComplete(request, ClassicCoreKvOps::commonKvResponseCheck, responseTransformer);
  }

  private <T extends BaseResponse, R> CompletableFuture<R> executeWithoutMarkingComplete(
      KeyValueRequest<T> request,
      BiConsumer<KeyValueRequest<T>, T> responseChecker,
      Function<T, R> responseTransformer
  ) {
    core.send(request);
    return request
        .response()
        .thenApply(response -> {
              if (!response.status().success()) {
                responseChecker.accept(request, response);
              }
              return responseTransformer.apply(response);
            }
        );
  }

  private static <T extends BaseResponse> void commonKvResponseCheck(KeyValueRequest<T> request, T response) {
    throw keyValueStatusToException(request, response);
  }

  private <T extends BaseResponse, R> CoreAsyncResponse<R> newAsyncResponse(
      KeyValueRequest<T> request,
      Function<T, R> responseTransformer
  ) {
    return newAsyncResponse(request, ClassicCoreKvOps::commonKvResponseCheck, responseTransformer);
  }

  private <T extends BaseResponse, R> CoreAsyncResponse<R> newAsyncResponse(
      KeyValueRequest<T> request,
      BiConsumer<KeyValueRequest<T>, T> responseChecker,
      Function<T, R> responseTransformer
  ) {
    CompletableFuture<R> response = execute(request, responseChecker, responseTransformer);
    return ClassicHelper.newAsyncResponse(request, response);
  }

  private static void markComplete(KeyValueRequest<?> request, Throwable failure) {
    if (failure == null || failure instanceof DocumentNotFoundException) {
      request.context().logicallyComplete();
    } else {
      request.context().logicallyComplete(failure);
    }
  }

  private Duration timeout(CoreCommonOptions common) {
    return common.timeout().orElse(defaultKvTimeout);
  }

  private Duration timeout(CoreCommonOptions common, CoreDurability durability) {
    return common.timeout().orElse(durability.isPersistent() ? defaultKvDurableTimeout : defaultKvTimeout);
  }

  private RetryStrategy retryStrategy(CoreCommonOptions common) {
    return common.retryStrategy().orElse(defaultRetryStrategy);
  }

  private RequestSpan span(CoreCommonOptions common, String spanName) {
    return span(common.parentSpan().orElse(null), spanName);
  }

  private RequestSpan span(RequestSpan parent, String spanName) {
    return CbTracing.newSpan(requestTracer, spanName, parent);
  }
}

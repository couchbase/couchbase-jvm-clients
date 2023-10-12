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

package com.couchbase.client.core.protostellar.manager;

import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.api.manager.search.CoreSearchIndex;
import com.couchbase.client.core.api.manager.search.CoreSearchIndexManager;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.ListenableFuture;
import com.couchbase.client.core.deps.com.google.protobuf.ByteString;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.protostellar.CoreProtostellarErrorHandlingUtil;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.protostellar.admin.search.v1.AllowIndexQueryingRequest;
import com.couchbase.client.protostellar.admin.search.v1.AnalyzeDocumentRequest;
import com.couchbase.client.protostellar.admin.search.v1.CreateIndexRequest;
import com.couchbase.client.protostellar.admin.search.v1.DeleteIndexRequest;
import com.couchbase.client.protostellar.admin.search.v1.DisallowIndexQueryingRequest;
import com.couchbase.client.protostellar.admin.search.v1.FreezeIndexPlanRequest;
import com.couchbase.client.protostellar.admin.search.v1.GetIndexRequest;
import com.couchbase.client.protostellar.admin.search.v1.GetIndexedDocumentsCountRequest;
import com.couchbase.client.protostellar.admin.search.v1.GetIndexedDocumentsCountResponse;
import com.couchbase.client.protostellar.admin.search.v1.Index;
import com.couchbase.client.protostellar.admin.search.v1.ListIndexesRequest;
import com.couchbase.client.protostellar.admin.search.v1.PauseIndexIngestRequest;
import com.couchbase.client.protostellar.admin.search.v1.ResumeIndexIngestRequest;
import com.couchbase.client.protostellar.admin.search.v1.SearchAdminServiceGrpc.SearchAdminServiceFutureStub;
import com.couchbase.client.protostellar.admin.search.v1.UnfreezeIndexPlanRequest;
import com.couchbase.client.protostellar.admin.search.v1.UpdateIndexRequest;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.couchbase.client.core.protostellar.CoreProtostellarAccessors.async;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;
import static com.couchbase.client.core.util.CbCollections.transform;
import static com.couchbase.client.core.util.CbCollections.transformValues;
import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class ProtostellarCoreSearchIndexManager implements CoreSearchIndexManager {
  private final CoreProtostellar core;
  @Nullable private final CoreBucketAndScope bucketAndScope;

  public ProtostellarCoreSearchIndexManager(
    CoreProtostellar protostellar,
    @Nullable CoreBucketAndScope bucketAndScope
  ) {
    this.core = requireNonNull(protostellar);
    this.bucketAndScope = bucketAndScope;
  }

  private RequestSpan span(CoreCommonOptions opts, String tracingIdentifier) {
    return createSpan(core, tracingIdentifier, CoreDurability.NONE, opts.parentSpan().orElse(null));
  }

  private Duration timeout(CoreCommonOptions opts) {
    return CoreProtostellarUtil.managementTimeout(opts.timeout(), core);
  }

  private RetryStrategy retryStrategy(CoreCommonOptions opts) {
    return opts.retryStrategy().orElse(core.context().environment().retryStrategy());
  }

  private enum ReadOnly {
    YES,
    NO,
  }

  private <T> ProtostellarRequest<T> protostellarRequest(T grpcRequest, CoreCommonOptions opts, String tracingIdentifier, ReadOnly readOnly) {
    return new ProtostellarRequest<>(
      grpcRequest,
      core,
      ServiceType.MANAGER,
      tracingIdentifier,
      span(opts, tracingIdentifier),
      timeout(opts),
      readOnly == ReadOnly.YES,
      retryStrategy(opts),
      opts.clientContext(),
      0,
      null
    );
  }

  private <TSdkResult, TGrpcRequest, TGrpcResponse> CoreAsyncResponse<TSdkResult> exec(
    ProtostellarRequest<TGrpcRequest> request,
    BiFunction<SearchAdminServiceFutureStub, TGrpcRequest, ListenableFuture<TGrpcResponse>> invocation,
    Function<TGrpcResponse, TSdkResult> convertResponse
  ) {
    return async(
      core,
      request,
      endpoint -> invocation.apply(endpoint.searchAdminStub().withDeadline(request.deadline()), request.request()),
      convertResponse,
      (err) -> CoreProtostellarErrorHandlingUtil.convertException(core, request, err)
    );
  }

  @Override
  public CompletableFuture<CoreSearchIndex> getIndex(String name, CoreCommonOptions options) {
    GetIndexRequest.Builder builder = GetIndexRequest.newBuilder()
      .setName(name);

    if (bucketAndScope != null) {
      builder.setBucketName(bucketAndScope.bucketName());
      builder.setScopeName(bucketAndScope.scopeName());
    }

    return exec(
      protostellarRequest(
        builder.build(),
        options,
        TracingIdentifiers.SPAN_REQUEST_MS_GET_INDEX,
        ReadOnly.YES
      ),
      SearchAdminServiceFutureStub::getIndex,
      response -> decodeIndex(response.getIndex())
    ).toFuture();
  }

  private static CoreSearchIndex decodeIndex(Index grpc) {
    return new CoreSearchIndex(
      grpc.getUuid(),
      grpc.getName(),
      grpc.getType(),
      decodeMap(grpc.getParamsMap()),
      grpc.getSourceUuid(),
      grpc.getSourceName(),
      decodeMap(grpc.getSourceParamsMap()),
      grpc.getSourceType(),
      decodeMap(grpc.getPlanParamsMap())
    );
  }

  private static Index.Builder encodeIndex(CoreSearchIndex index) {
    return Index.newBuilder()
      .setUuid(index.uuid())
      .setName(index.name())
      .setType(index.type())
      .putAllParams(encodeMap(index.params()))
      .setSourceUuid(index.sourceUuid())
      .setSourceName(index.sourceName())
      .putAllSourceParams(encodeMap(index.sourceParams()))
      .setSourceType(index.sourceType())
      .putAllPlanParams(encodeMap(index.planParams()));
  }

  private void encodeIndex(CreateIndexRequest.Builder builder, CoreSearchIndex index) {
    builder
      .setName(index.name())
      .setType(index.type())
      .putAllParams(encodeMap(index.params()))
      .setSourceUuid(index.sourceUuid())
      .setSourceName(index.sourceName())
      .putAllSourceParams(encodeMap(index.sourceParams()))
      .setSourceType(index.sourceType())
      .putAllPlanParams(encodeMap(index.planParams()));
  }

  private static Map<String, Object> decodeMap(Map<String, ByteString> grpc) {
    return transformValues(grpc, value -> Mapper.decodeInto(value.toByteArray(), Object.class));
  }

  private static Map<String, ByteString> encodeMap(Map<String, Object> sdk) {
    return transformValues(sdk, value -> ByteString.copyFrom(Mapper.encodeAsBytes(value)));
  }

  @Override
  public CompletableFuture<List<CoreSearchIndex>> getAllIndexes(CoreCommonOptions options) {
    ListIndexesRequest.Builder builder = ListIndexesRequest.newBuilder();

    if (bucketAndScope != null) {
      builder.setBucketName(bucketAndScope.bucketName());
      builder.setScopeName(bucketAndScope.scopeName());
    }

    return exec(
      protostellarRequest(
        builder.build(),
        options,
        TracingIdentifiers.SPAN_REQUEST_MS_GET_ALL_INDEXES,
        ReadOnly.YES
      ),
      SearchAdminServiceFutureStub::listIndexes,
      response -> transform(response.getIndexesList(), ProtostellarCoreSearchIndexManager::decodeIndex)
    ).toFuture();
  }

  @Override
  public CompletableFuture<Long> getIndexedDocumentsCount(String name, CoreCommonOptions options) {
    GetIndexedDocumentsCountRequest.Builder builder = GetIndexedDocumentsCountRequest.newBuilder()
      .setName(name);

    if (bucketAndScope != null) {
      builder.setBucketName(bucketAndScope.bucketName());
      builder.setScopeName(bucketAndScope.scopeName());
    }

    return exec(
      protostellarRequest(
        builder.build(),
        options,
        TracingIdentifiers.SPAN_REQUEST_MS_GET_IDX_DOC_COUNT,
        ReadOnly.YES
      ),
      SearchAdminServiceFutureStub::getIndexedDocumentsCount,
      GetIndexedDocumentsCountResponse::getCount
    ).toFuture();
  }

  @Override
  public CompletableFuture<Void> upsertIndex(CoreSearchIndex index, CoreCommonOptions options) {
    return isNullOrEmpty(index.uuid())
      ? createIndex(index, options)
      : updateIndex(index, options);
  }

  private CompletableFuture<Void> createIndex(CoreSearchIndex index, CoreCommonOptions options) {
    CreateIndexRequest.Builder builder = CreateIndexRequest.newBuilder();
    encodeIndex(builder, index);

    if (bucketAndScope != null) {
      builder.setBucketName(bucketAndScope.bucketName());
      builder.setScopeName(bucketAndScope.scopeName());
    }

    return exec(
      protostellarRequest(
        builder.build(),
        options,
        TracingIdentifiers.SPAN_REQUEST_MS_UPSERT_INDEX,
        ReadOnly.NO
      ),
      SearchAdminServiceFutureStub::createIndex,
      response -> null
    ).toFutureVoid();
  }

  private CompletableFuture<Void> updateIndex(CoreSearchIndex index, CoreCommonOptions options) {
    UpdateIndexRequest.Builder builder = UpdateIndexRequest.newBuilder()
      .setIndex(encodeIndex(index));

    if (bucketAndScope != null) {
      builder.setBucketName(bucketAndScope.bucketName());
      builder.setScopeName(bucketAndScope.scopeName());
    }

    return exec(
      protostellarRequest(
        builder.build(),
        options,
        TracingIdentifiers.SPAN_REQUEST_MS_UPSERT_INDEX,
        ReadOnly.NO
      ),
      SearchAdminServiceFutureStub::updateIndex,
      response -> null
    ).toFutureVoid();
  }

  @Override
  public CompletableFuture<Void> dropIndex(String name, CoreCommonOptions options) {
    DeleteIndexRequest.Builder builder = DeleteIndexRequest.newBuilder()
      .setName(name);

    if (bucketAndScope != null) {
      builder.setBucketName(bucketAndScope.bucketName());
      builder.setScopeName(bucketAndScope.scopeName());
    }

    return exec(
      protostellarRequest(
        builder.build(),
        options,
        TracingIdentifiers.SPAN_REQUEST_MS_DROP_INDEX,
        ReadOnly.NO
      ),
      SearchAdminServiceFutureStub::deleteIndex,
      response -> null
    ).toFutureVoid();
  }

  @Override
  public CompletableFuture<List<ObjectNode>> analyzeDocument(String name, ObjectNode document, CoreCommonOptions options) {
    AnalyzeDocumentRequest.Builder builder = AnalyzeDocumentRequest.newBuilder()
      .setName(name)
      .setDoc(ByteString.copyFrom(Mapper.encodeAsBytes(document)));

    if (bucketAndScope != null) {
      builder.setBucketName(bucketAndScope.bucketName());
      builder.setScopeName(bucketAndScope.scopeName());
    }

    return exec(
      protostellarRequest(
        builder.build(),
        options,
        TracingIdentifiers.SPAN_REQUEST_MS_ANALYZE_DOCUMENT,
        ReadOnly.YES
      ),
      SearchAdminServiceFutureStub::analyzeDocument,
      response -> Mapper.decodeInto(response.getAnalyzed().toByteArray(), new TypeReference<List<ObjectNode>>() {})
    ).toFuture();
  }

  @Override
  public CompletableFuture<Void> pauseIngest(String name, CoreCommonOptions options) {
    PauseIndexIngestRequest.Builder builder = PauseIndexIngestRequest.newBuilder()
      .setName(name);

    if (bucketAndScope != null) {
      builder.setBucketName(bucketAndScope.bucketName());
      builder.setScopeName(bucketAndScope.scopeName());
    }

    return exec(
      protostellarRequest(
        builder.build(),
        options,
        TracingIdentifiers.SPAN_REQUEST_MS_PAUSE_INGEST,
        ReadOnly.NO
      ),
      SearchAdminServiceFutureStub::pauseIndexIngest,
      response -> null
    ).toFutureVoid();
  }

  @Override
  public CompletableFuture<Void> resumeIngest(String name, CoreCommonOptions options) {
    ResumeIndexIngestRequest.Builder builder = ResumeIndexIngestRequest.newBuilder()
      .setName(name);

    if (bucketAndScope != null) {
      builder.setBucketName(bucketAndScope.bucketName());
      builder.setScopeName(bucketAndScope.scopeName());
    }

    return exec(
      protostellarRequest(
        builder.build(),
        options,
        TracingIdentifiers.SPAN_REQUEST_MS_RESUME_INGEST,
        ReadOnly.NO
      ),
      SearchAdminServiceFutureStub::resumeIndexIngest,
      response -> null
    ).toFutureVoid();
  }

  @Override
  public CompletableFuture<Void> allowQuerying(String name, CoreCommonOptions options) {
    AllowIndexQueryingRequest.Builder builder = AllowIndexQueryingRequest.newBuilder()
      .setName(name);

    if (bucketAndScope != null) {
      builder.setBucketName(bucketAndScope.bucketName());
      builder.setScopeName(bucketAndScope.scopeName());
    }

    return exec(
      protostellarRequest(
        builder.build(),
        options,
        TracingIdentifiers.SPAN_REQUEST_MS_ALLOW_QUERYING,
        ReadOnly.NO
      ),
      SearchAdminServiceFutureStub::allowIndexQuerying,
      response -> null
    ).toFutureVoid();
  }

  @Override
  public CompletableFuture<Void> disallowQuerying(String name, CoreCommonOptions options) {
    DisallowIndexQueryingRequest.Builder builder = DisallowIndexQueryingRequest.newBuilder()
      .setName(name);

    if (bucketAndScope != null) {
      builder.setBucketName(bucketAndScope.bucketName());
      builder.setScopeName(bucketAndScope.scopeName());
    }

    return exec(
      protostellarRequest(
        builder.build(),
        options,
        TracingIdentifiers.SPAN_REQUEST_MS_DISALLOW_QUERYING,
        ReadOnly.NO
      ),
      SearchAdminServiceFutureStub::disallowIndexQuerying,
      response -> null
    ).toFutureVoid();
  }

  @Override
  public CompletableFuture<Void> freezePlan(String name, CoreCommonOptions options) {
    FreezeIndexPlanRequest.Builder builder = FreezeIndexPlanRequest.newBuilder()
      .setName(name);

    if (bucketAndScope != null) {
      builder.setBucketName(bucketAndScope.bucketName());
      builder.setScopeName(bucketAndScope.scopeName());
    }

    return exec(
      protostellarRequest(
        builder.build(),
        options,
        TracingIdentifiers.SPAN_REQUEST_MS_FREEZE_PLAN,
        ReadOnly.NO
      ),
      SearchAdminServiceFutureStub::freezeIndexPlan,
      response -> null
    ).toFutureVoid();
  }

  @Override
  public CompletableFuture<Void> unfreezePlan(String name, CoreCommonOptions options) {
    UnfreezeIndexPlanRequest.Builder builder = UnfreezeIndexPlanRequest.newBuilder()
      .setName(name);

    if (bucketAndScope != null) {
      builder.setBucketName(bucketAndScope.bucketName());
      builder.setScopeName(bucketAndScope.scopeName());
    }

    return exec(
      protostellarRequest(
        builder.build(),
        options,
        TracingIdentifiers.SPAN_REQUEST_MS_UNFREEZE_PLAN,
        ReadOnly.NO
      ),
      SearchAdminServiceFutureStub::unfreezeIndexPlan,
      response -> null
    ).toFutureVoid();
  }
}

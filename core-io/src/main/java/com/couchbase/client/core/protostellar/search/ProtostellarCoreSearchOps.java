/*
 * Copyright (c) 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.protostellar.search;

import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.api.search.CoreSearchMetaData;
import com.couchbase.client.core.api.search.CoreSearchOps;
import com.couchbase.client.core.api.search.CoreSearchOptions;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.api.search.CoreSearchScanConsistency;
import com.couchbase.client.core.api.search.result.CoreReactiveSearchResult;
import com.couchbase.client.core.api.search.result.CoreSearchMetrics;
import com.couchbase.client.core.api.search.result.CoreSearchResult;
import com.couchbase.client.core.api.search.result.CoreSearchRow;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.io.grpc.stub.StreamObserver;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.protostellar.search.v1.SearchQueryRequest;
import com.couchbase.client.protostellar.search.v1.SearchQueryResponse;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownAsync;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownReactive;
import static com.couchbase.client.core.util.ProtostellarUtil.convert;
import static java.util.Objects.requireNonNull;


@Stability.Internal
public class ProtostellarCoreSearchOps implements CoreSearchOps {
  private final CoreProtostellar core;

  public ProtostellarCoreSearchOps(CoreProtostellar core, @Nullable CoreBucketAndScope scope) {
    this.core = requireNonNull(core);

    // scope is silently ignored as it requires ING-381.
    // throwing here would require creating a new CoreSearchOps object on every search operation.
  }

  @Override
  public CoreAsyncResponse<CoreSearchResult> searchQueryAsync(String indexName,
                                                              CoreSearchQuery search,
                                                              CoreSearchOptions options) {
    ProtostellarRequest<SearchQueryRequest> request = request(core, indexName, search, options);
    CompletableFuture<CoreSearchResult> ret = new CompletableFuture<>();
    CoreAsyncResponse<CoreSearchResult> out = new CoreAsyncResponse<>(ret, () -> {
    });
    if (handleShutdownAsync(core, ret, request)) {
      return out;
    }
    List<SearchQueryResponse> responses = new ArrayList<>();

    StreamObserver<SearchQueryResponse> response = new StreamObserver<SearchQueryResponse>() {
      @Override
      public void onNext(SearchQueryResponse response) {
        responses.add(response);
      }

      @Override
      public void onError(Throwable throwable) {
        ret.completeExceptionally(convertException(throwable));
      }

      @Override
      public void onCompleted() {
        List<CoreSearchRow> rows = new ArrayList<>();
        CoreSearchMetaData metaData = null;

        for (SearchQueryResponse r : responses) {
          if (r.hasMetaData()) {
            SearchQueryResponse.MetaData md = r.getMetaData();
            // errors require ING-381
            SearchQueryResponse.SearchMetrics metrics = md.getMetrics();
            metaData = new CoreSearchMetaData(Collections.emptyMap(), new CoreSearchMetrics(
              convert(metrics.getExecutionTime()),
              metrics.getTotalRows(),
              metrics.getMaxScore(),
              metrics.getSuccessPartitionCount(),
              metrics.getErrorPartitionCount())
            );
          }

          r.getHitsList()
                  .forEach(hit -> {
                    ObjectNode json = (ObjectNode) Mapper.decodeIntoTree(hit.toByteArray());
                    rows.add(CoreSearchRow.fromResponse(json));
                  });
        }

        // facets require ING-381
        ret.complete(new CoreSearchResult(rows, Collections.emptyMap(), metaData));
      }
    };

    core.endpoint().searchStub()
            .withDeadline(request.deadline())
            .searchQuery(request.request(), response);

    return out;
  }

  @Override
  public Mono<CoreReactiveSearchResult> searchQueryReactive(String indexName,
                                                            CoreSearchQuery query,
                                                            CoreSearchOptions options) {
    return Mono.defer(() -> {
      ProtostellarRequest<SearchQueryRequest> request = request(core, indexName, query, options);
      Mono<CoreReactiveSearchResult> err = handleShutdownReactive(core, request);
      if (err != null) {
        return err;
      }

      Sinks.Many<CoreSearchRow> rows = Sinks.many().replay().latest();
      Sinks.One<CoreSearchMetaData> metaData = Sinks.one();

      StreamObserver<SearchQueryResponse> response = new StreamObserver<SearchQueryResponse>() {
        @Override
        public void onNext(SearchQueryResponse response) {
          response.getHitsList().forEach(hit -> {
            ObjectNode json = (ObjectNode) Mapper.decodeIntoTree(hit.toByteArray());
            CoreSearchRow row = CoreSearchRow.fromResponse(json);
            rows.tryEmitNext(row).orThrow();
          });

          if (response.hasMetaData()) {
            SearchQueryResponse.MetaData md = response.getMetaData();
            SearchQueryResponse.SearchMetrics metrics = md.getMetrics();
            CoreSearchMetaData cmd = new CoreSearchMetaData(Collections.emptyMap(), new CoreSearchMetrics(
              convert(metrics.getExecutionTime()),
              metrics.getTotalRows(),
              metrics.getMaxScore(),
              metrics.getSuccessPartitionCount(),
              metrics.getErrorPartitionCount()
            ));
            metaData.tryEmitValue(cmd).orThrow();
          }
        }

        @Override
        public void onError(Throwable throwable) {
          rows.tryEmitError(convertException(throwable)).orThrow();
        }

        @Override
        public void onCompleted() {
          rows.tryEmitComplete().orThrow();
        }
      };

      core.endpoint().searchStub()
              .withDeadline(request.deadline())
              .searchQuery(request.request(), response);

      return Mono.just(new CoreReactiveSearchResult(rows.asFlux(), Mono.empty(), metaData.asMono()));
    });
  }

  private static RuntimeException convertException(Throwable throwable) {
    // STG does not currently implement most search errors.  Once it does, will want to pass throwable through CoreProtostellarErrorHandlingUtil.
    if (throwable instanceof RuntimeException) {
      return (RuntimeException) throwable;
    }
    return new RuntimeException(throwable);
  }

  private static ProtostellarRequest<SearchQueryRequest> request(CoreProtostellar core,
                                                                 String indexName,
                                                                 CoreSearchQuery query,
                                                                 CoreSearchOptions opts) {
    Duration timeout = opts.commonOptions().timeout().orElse(core.context().environment().timeoutConfig().queryTimeout());
    RequestSpan span = createSpan(core, TracingIdentifiers.SPAN_REQUEST_SEARCH, CoreDurability.NONE, opts.commonOptions().parentSpan().orElse(null));

    SearchQueryRequest.Builder request = SearchQueryRequest.newBuilder()
            .setIndexName(indexName)
            .setQuery(query.asProtostellar());

    if (opts.consistency() != null) {
      if (opts.consistency() == CoreSearchScanConsistency.NOT_BOUNDED) {
        request.setScanConsistency(SearchQueryRequest.ScanConsistency.SCAN_CONSISTENCY_NOT_BOUNDED);
      }
    }

    if (opts.limit() != null) {
      request.setLimit(opts.limit());
    }

    if (opts.skip() != null) {
      request.setSkip(opts.skip());
    }

    if (opts.explain() != null) {
      request.setIncludeExplanation(opts.explain());
    }

    if (opts.highlightStyle() != null) {
      switch (opts.highlightStyle()) {
        case HTML:
          request.setHighlightStyle(SearchQueryRequest.HighlightStyle.HIGHLIGHT_STYLE_HTML);
          break;
        case ANSI:
          request.setHighlightStyle(SearchQueryRequest.HighlightStyle.HIGHLIGHT_STYLE_ANSI);
          break;
        case SERVER_DEFAULT:
          request.setHighlightStyle(SearchQueryRequest.HighlightStyle.HIGHLIGHT_STYLE_DEFAULT);
          break;
      }
    }

    if (!opts.highlightFields().isEmpty()) {
      request.addAllHighlightFields(opts.highlightFields());
    }

    if (!opts.fields().isEmpty()) {
      request.addAllFields(opts.fields());
    }

    opts.sort().forEach(sort -> request.addSort(sort.asProtostellar()));

    if (!opts.sortString().isEmpty()) {
      // Requires ING-381
      throw new UnsupportedOperationException("Sorting with strings is not currently supported in Protostellar");
    }

    if (opts.disableScoring() != null) {
      request.setDisableScoring(opts.disableScoring());
    }

    if (!opts.collections().isEmpty()) {
      request.addAllCollections(opts.collections());
    }

    if (opts.includeLocations() != null) {
      request.setIncludeExplanation(opts.includeLocations());
    }

    return new ProtostellarRequest<>(
      request.build(),
      core,
      ServiceType.SEARCH,
      TracingIdentifiers.SPAN_REQUEST_SEARCH,
      span,
      timeout,
      false,
      opts.commonOptions().retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.commonOptions().clientContext(),
      0L,
      null
    );

  }
}

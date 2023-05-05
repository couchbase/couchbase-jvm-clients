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
import com.couchbase.client.core.api.search.CoreSearchKeyset;
import com.couchbase.client.core.api.search.CoreSearchMetaData;
import com.couchbase.client.core.api.search.CoreSearchOps;
import com.couchbase.client.core.api.search.CoreSearchOptions;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.api.search.CoreSearchScanConsistency;
import com.couchbase.client.core.api.search.facet.CoreDateRange;
import com.couchbase.client.core.api.search.facet.CoreDateRangeFacet;
import com.couchbase.client.core.api.search.facet.CoreNumericRange;
import com.couchbase.client.core.api.search.facet.CoreNumericRangeFacet;
import com.couchbase.client.core.api.search.facet.CoreSearchFacet;
import com.couchbase.client.core.api.search.facet.CoreTermFacet;
import com.couchbase.client.core.api.search.result.CoreDateRangeSearchFacetResult;
import com.couchbase.client.core.api.search.result.CoreNumericRangeSearchFacetResult;
import com.couchbase.client.core.api.search.result.CoreReactiveSearchResult;
import com.couchbase.client.core.api.search.result.CoreSearchDateRange;
import com.couchbase.client.core.api.search.result.CoreSearchFacetResult;
import com.couchbase.client.core.api.search.result.CoreSearchMetrics;
import com.couchbase.client.core.api.search.result.CoreSearchNumericRange;
import com.couchbase.client.core.api.search.result.CoreSearchResult;
import com.couchbase.client.core.api.search.result.CoreSearchRow;
import com.couchbase.client.core.api.search.result.CoreSearchRowLocation;
import com.couchbase.client.core.api.search.result.CoreSearchRowLocations;
import com.couchbase.client.core.api.search.result.CoreSearchTermRange;
import com.couchbase.client.core.api.search.result.CoreTermSearchFacetResult;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.com.google.protobuf.ByteString;
import com.couchbase.client.core.deps.com.google.protobuf.Timestamp;
import com.couchbase.client.core.deps.io.grpc.stub.StreamObserver;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.protostellar.search.v1.DateRange;
import com.couchbase.client.protostellar.search.v1.DateRangeFacet;
import com.couchbase.client.protostellar.search.v1.Facet;
import com.couchbase.client.protostellar.search.v1.NumericRange;
import com.couchbase.client.protostellar.search.v1.NumericRangeFacet;
import com.couchbase.client.protostellar.search.v1.SearchQueryRequest;
import com.couchbase.client.protostellar.search.v1.SearchQueryResponse;
import com.couchbase.client.protostellar.search.v1.TermFacet;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownAsync;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownReactive;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.unsupportedInProtostellar;
import static com.couchbase.client.core.util.CbCollections.transform;
import static com.couchbase.client.core.util.CbCollections.transformValues;
import static com.couchbase.client.core.util.ProtostellarUtil.convert;
import static java.util.Collections.emptyMap;
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
    options.validate();

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
        Map<String, CoreSearchFacetResult> facets = emptyMap();

        for (SearchQueryResponse r : responses) {
          if (r.hasMetaData()) {
            metaData = parseMetadata(r);
            facets = parseFacets(r);
          }

          r.getHitsList()
            .forEach(hit -> rows.add(parse(hit)));
        }

        ret.complete(new CoreSearchResult(rows, facets, metaData));
      }
    };

    core.endpoint().searchStub()
      .withDeadline(request.deadline())
      .searchQuery(request.request(), response);

    return out;
  }

  private static CoreSearchMetaData parseMetadata(SearchQueryResponse response) {
    SearchQueryResponse.MetaData md = response.getMetaData();
    SearchQueryResponse.SearchMetrics metrics = md.getMetrics();

    return new CoreSearchMetaData(
      md.getErrorsMap(),
      new CoreSearchMetrics(
        convert(metrics.getExecutionTime()),
        metrics.getTotalRows(),
        metrics.getMaxScore(),
        metrics.getSuccessPartitionCount(),
        metrics.getErrorPartitionCount()
      )
    );
  }

  @Override
  public Mono<CoreReactiveSearchResult> searchQueryReactive(String indexName,
                                                            CoreSearchQuery query,
                                                            CoreSearchOptions options) {
    options.validate();

    return Mono.defer(() -> {
      ProtostellarRequest<SearchQueryRequest> request = request(core, indexName, query, options);
      Mono<CoreReactiveSearchResult> err = handleShutdownReactive(core, request);
      if (err != null) {
        return err;
      }

      Sinks.Many<CoreSearchRow> rows = Sinks.many().replay().latest();
      Sinks.One<CoreSearchMetaData> metaData = Sinks.one();
      Sinks.One<Map<String, CoreSearchFacetResult>> facets = Sinks.one();

      StreamObserver<SearchQueryResponse> response = new StreamObserver<SearchQueryResponse>() {
        @Override
        public void onNext(SearchQueryResponse response) {
          response.getHitsList().forEach(hit -> {
            CoreSearchRow row = parse(hit);
            rows.tryEmitNext(row).orThrow();
          });

          if (response.hasMetaData()) {
            CoreSearchMetaData cmd = parseMetadata(response);
            metaData.tryEmitValue(cmd).orThrow();

            if (response.getFacetsCount() > 0) {
              Map<String, CoreSearchFacetResult> coreFacets = parseFacets(response);
              facets.tryEmitValue(coreFacets).orThrow();
            } else {
              facets.tryEmitValue(emptyMap()).orThrow();
            }
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

      return Mono.just(new CoreReactiveSearchResult(rows.asFlux(), facets.asMono(), metaData.asMono()));
    });
  }

  private static Map<String, CoreSearchFacetResult> parseFacets(SearchQueryResponse response) {
    return transformValues(response.getFacetsMap(), ProtostellarCoreSearchOps::convertFacetResult);
  }

  private static CoreSearchFacetResult convertFacetResult(SearchQueryResponse.FacetResult facet) {
    if (facet.hasTermFacet()) {
      SearchQueryResponse.TermFacetResult result = facet.getTermFacet();
      return new CoreTermSearchFacetResult(
        result.getName(),
        result.getField(),
        result.getTotal(),
        result.getMissing(),
        result.getOther(),
        transform(result.getTermsList(), it -> new CoreSearchTermRange(
          it.getName(),
          it.getSize()
        ))
      );
    }

    if (facet.hasNumericRangeFacet()) {
      SearchQueryResponse.NumericRangeFacetResult result = facet.getNumericRangeFacet();
      return new CoreNumericRangeSearchFacetResult(
        result.getName(),
        result.getField(),
        result.getTotal(),
        result.getMissing(),
        result.getOther(),
        transform(result.getNumericRangesList(), it -> new CoreSearchNumericRange(
          it.getName(),
          parseNumericRangeEndpoint(it.getMin()),
          parseNumericRangeEndpoint(it.getMax()),
          it.getSize()
        ))
      );
    }

    if (facet.hasDateRangeFacet()) {
      SearchQueryResponse.DateRangeFacetResult result = facet.getDateRangeFacet();
      return new CoreDateRangeSearchFacetResult(
        result.getName(),
        result.getField(),
        result.getTotal(),
        result.getMissing(),
        result.getOther(),
        transform(result.getDateRangesList(), it -> new CoreSearchDateRange(
          it.getName(),
          it.hasStart() ? toInstant(it.getStart()) : null,
          it.hasEnd() ? toInstant(it.getEnd()) : null,
          it.getSize()
        ))
      );
    }

    throw new RuntimeException("Unexpected facet result type: " + facet);
  }

  /**
   * Workaround for a Protostellar API bug where these endpoints are represented
   * as `long` instead of `Double`.
   * <p>
   * TODO: remove this method once NumericRangeResult.min() and max() return Double.
   * Might need to have caller add a hasMin() / hasMax() check as well.
   */
  private static Double parseNumericRangeEndpoint(@Nullable Number n) {
    return n == null ? null : n.doubleValue();
  }

  private static CoreSearchRow parse(SearchQueryResponse.SearchQueryRow row) {
    return new CoreSearchRow(
      row.getIndex(),
      row.getId(),
      row.getScore(),
      parseExplanation(row),
      parseLocations(row),
      parseFragments(row),
      parseFields(row),
      () -> CoreSearchKeyset.EMPTY // pending ING-476
    );
  }

  private static ObjectNode parseExplanation(SearchQueryResponse.SearchQueryRow hit) {
    return hit.getExplanation().isEmpty()
      ? Mapper.createObjectNode()
      : (ObjectNode) Mapper.decodeIntoTree(hit.getExplanation().toByteArray());
  }

  private static byte[] parseFields(SearchQueryResponse.SearchQueryRow hit) {
    // Turn this map back into a JSON Object.
    Map<String, ByteString> fields = hit.getFieldsMap();
    if (fields.isEmpty()) {
      return new byte[]{'{', '}'};
    }

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    os.write('{');

    hit.getFieldsMap().forEach((key, value) -> {
      try {
        Mapper.writer().writeValue(os, key);
        os.write(':');
        os.write(value.toByteArray());
        os.write(',');
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    byte[] result = os.toByteArray();
    result[result.length - 1] = '}'; // overwrite trailing comma
    return result;
  }

  private static Map<String, List<String>> parseFragments(SearchQueryResponse.SearchQueryRow row) {
    return transformValues(row.getFragmentsMap(), SearchQueryResponse.Fragment::getContentList);
  }

  private static Optional<CoreSearchRowLocations> parseLocations(SearchQueryResponse.SearchQueryRow row) {
    if (row.getLocationsCount() == 0) {
      return Optional.empty();
    }

    List<CoreSearchRowLocation> result = new ArrayList<>(row.getLocationsCount());
    row.getLocationsList().forEach(loc -> result.add(parseOneLocation(loc)));
    return Optional.of(CoreSearchRowLocations.from(result));
  }

  private static CoreSearchRowLocation parseOneLocation(SearchQueryResponse.Location loc) {
    return new CoreSearchRowLocation(
      loc.getField(),
      loc.getTerm(),
      loc.getPosition(),
      loc.getStart(),
      loc.getEnd(),
      loc.getArrayPositionsCount() == 0 ? null : toPrimitiveLongArray(loc.getArrayPositionsList())
    );
  }

  private static long[] toPrimitiveLongArray(List<? extends Number> list) {
    long[] result = new long[list.size()];
    for (ListIterator<? extends Number> i = list.listIterator(); i.hasNext(); ) {
      result[i.nextIndex()] = i.next().longValue();
    }
    return result;
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

    if (opts.searchBefore() != null || opts.searchAfter() != null) {
      throw unsupportedInProtostellar("keyset pagination with searchBefore/After");
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

    if (opts.disableScoring() != null) {
      request.setDisableScoring(opts.disableScoring());
    }

    if (!opts.collections().isEmpty()) {
      request.addAllCollections(opts.collections());
    }

    if (opts.includeLocations() != null) {
      request.setIncludeExplanation(opts.includeLocations());
    }

    opts.facets().forEach((name, facet) -> request.putFacets(name, convertFacet(facet)));

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

  private static Facet convertFacet(CoreSearchFacet facet) {
    if (facet instanceof CoreTermFacet) {
      return convertTermFacet(facet);
    }

    if (facet instanceof CoreNumericRangeFacet) {
      return convertNumericRangeFacet(facet);
    }

    if (facet instanceof CoreDateRangeFacet) {
      return convertDateRangeFacet(facet);
    }

    throw new RuntimeException("Unexpected facet type: " + facet.getClass());
  }

  @NonNull
  private static Facet convertDateRangeFacet(CoreSearchFacet facet) {
    DateRangeFacet.Builder builder = DateRangeFacet.newBuilder()
      .setField(facet.field());
    Integer size = facet.size();
    if (size != null) {
      builder.setSize(size);
    }
    List<CoreDateRange> coreRanges = ((CoreDateRangeFacet) facet).dateRanges();
    coreRanges.forEach(it -> builder.addDateRanges(convertDateRange(it)));

    return Facet.newBuilder()
      .setDateRangeFacet(builder)
      .build();
  }

  @NonNull
  private static Facet convertNumericRangeFacet(CoreSearchFacet facet) {
    NumericRangeFacet.Builder builder = NumericRangeFacet.newBuilder()
      .setField(facet.field());
    Integer size = facet.size();
    if (size != null) {
      builder.setSize(size);
    }
    List<CoreNumericRange> coreRanges = ((CoreNumericRangeFacet) facet).ranges();
    coreRanges.forEach(it -> builder.addNumericRanges(convertNumericRange(it)));

    return Facet.newBuilder()
      .setNumericRangeFacet(builder)
      .build();
  }

  @NonNull
  private static Facet convertTermFacet(CoreSearchFacet facet) {
    TermFacet.Builder builder = TermFacet.newBuilder()
      .setField(facet.field());
    Integer size = facet.size();
    if (size != null) {
      builder.setSize(size);
    }

    return Facet.newBuilder()
      .setTermFacet(builder)
      .build();
  }

  private static DateRange.Builder convertDateRange(CoreDateRange range) {
    DateRange.Builder builder = DateRange.newBuilder()
      .setName(range.name());

    String start = range.start();
    if (start != null) {
      builder.setStart(start);
    }

    String end = range.end();
    if (end != null) {
      builder.setEnd(end);
    }

    return builder;
  }

  private static NumericRange.Builder convertNumericRange(CoreNumericRange range) {
    NumericRange.Builder builder = NumericRange.newBuilder()
      .setName(range.name());

    Double min = range.min();
    if (min != null) {
      builder.setMin(min.floatValue());
    }

    Double max = range.max();
    if (max != null) {
      builder.setMax(max.floatValue());
    }

    return builder;
  }

  private static Instant toInstant(Timestamp ts) {
    return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
  }
}

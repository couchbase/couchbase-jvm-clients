/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase.client.core.protostellar.search;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.api.search.queries.CoreBooleanFieldQuery;
import com.couchbase.client.core.api.search.queries.CoreBooleanQuery;
import com.couchbase.client.core.api.search.queries.CoreConjunctionQuery;
import com.couchbase.client.core.api.search.queries.CoreCustomQuery;
import com.couchbase.client.core.api.search.queries.CoreDateRangeQuery;
import com.couchbase.client.core.api.search.queries.CoreDisjunctionQuery;
import com.couchbase.client.core.api.search.queries.CoreDocIdQuery;
import com.couchbase.client.core.api.search.queries.CoreGeoBoundingBoxQuery;
import com.couchbase.client.core.api.search.queries.CoreGeoDistanceQuery;
import com.couchbase.client.core.api.search.queries.CoreGeoPolygonQuery;
import com.couchbase.client.core.api.search.queries.CoreMatchAllQuery;
import com.couchbase.client.core.api.search.queries.CoreMatchNoneQuery;
import com.couchbase.client.core.api.search.queries.CoreMatchOperator;
import com.couchbase.client.core.api.search.queries.CoreMatchPhraseQuery;
import com.couchbase.client.core.api.search.queries.CoreMatchQuery;
import com.couchbase.client.core.api.search.queries.CoreNumericRangeQuery;
import com.couchbase.client.core.api.search.queries.CorePhraseQuery;
import com.couchbase.client.core.api.search.queries.CorePrefixQuery;
import com.couchbase.client.core.api.search.queries.CoreQueryStringQuery;
import com.couchbase.client.core.api.search.queries.CoreRegexpQuery;
import com.couchbase.client.core.api.search.queries.CoreSearchQueryConverter;
import com.couchbase.client.core.api.search.queries.CoreTermQuery;
import com.couchbase.client.core.api.search.queries.CoreTermRangeQuery;
import com.couchbase.client.core.api.search.queries.CoreWildcardQuery;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.protostellar.search.v1.BooleanFieldQuery;
import com.couchbase.client.protostellar.search.v1.BooleanQuery;
import com.couchbase.client.protostellar.search.v1.ConjunctionQuery;
import com.couchbase.client.protostellar.search.v1.DateRangeQuery;
import com.couchbase.client.protostellar.search.v1.DisjunctionQuery;
import com.couchbase.client.protostellar.search.v1.DocIdQuery;
import com.couchbase.client.protostellar.search.v1.GeoBoundingBoxQuery;
import com.couchbase.client.protostellar.search.v1.GeoDistanceQuery;
import com.couchbase.client.protostellar.search.v1.GeoPolygonQuery;
import com.couchbase.client.protostellar.search.v1.MatchAllQuery;
import com.couchbase.client.protostellar.search.v1.MatchNoneQuery;
import com.couchbase.client.protostellar.search.v1.MatchPhraseQuery;
import com.couchbase.client.protostellar.search.v1.MatchQuery;
import com.couchbase.client.protostellar.search.v1.NumericRangeQuery;
import com.couchbase.client.protostellar.search.v1.PhraseQuery;
import com.couchbase.client.protostellar.search.v1.PrefixQuery;
import com.couchbase.client.protostellar.search.v1.Query;
import com.couchbase.client.protostellar.search.v1.QueryStringQuery;
import com.couchbase.client.protostellar.search.v1.RegexpQuery;
import com.couchbase.client.protostellar.search.v1.TermQuery;
import com.couchbase.client.protostellar.search.v1.TermRangeQuery;
import com.couchbase.client.protostellar.search.v1.WildcardQuery;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.function.Consumer;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.toLatLng;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.unsupportedInProtostellar;
import static com.couchbase.client.core.util.CbCollections.transform;

@NullMarked
@Stability.Internal
public class ProtostellarSearchQueryConverter implements CoreSearchQueryConverter<Query> {
  public static final ProtostellarSearchQueryConverter instance = new ProtostellarSearchQueryConverter();

  private ProtostellarSearchQueryConverter() {
  }

  private static @Nullable Float boostAsFloat(CoreSearchQuery core) {
    Double boost = core.boost;
    return boost == null ? null : boost.floatValue();
  }

  private static <T> void ifNotNull(@Nullable T value, Consumer<T> action) {
    if (value != null) action.accept(value);
  }

  @Override
  public Query convert(CoreBooleanFieldQuery core) {
    BooleanFieldQuery.Builder builder = BooleanFieldQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);
    ifNotNull(core.field, builder::setField);

    builder.setValue(core.value);

    return Query.newBuilder().setBooleanFieldQuery(builder).build();
  }

  @Override
  public Query convert(CoreBooleanQuery core) {
    BooleanQuery.Builder builder = BooleanQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);

    ifNotNull(core.must, it -> builder.setMust(convert(it).getConjunctionQuery()));
    ifNotNull(core.mustNot, it -> builder.setMustNot(convert(it).getDisjunctionQuery()));
    ifNotNull(core.should, it -> builder.setShould(convert(it).getDisjunctionQuery()));

    return Query.newBuilder().setBooleanQuery(builder).build();
  }

  @Override
  public Query convert(CoreConjunctionQuery core) {
    ConjunctionQuery.Builder builder = ConjunctionQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);

    builder.addAllQueries(transform(core.childQueries(), it -> it.convert(this)));

    return Query.newBuilder().setConjunctionQuery(builder).build();
  }

  @Override
  public Query convert(CoreDisjunctionQuery core) {
    DisjunctionQuery.Builder builder = DisjunctionQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);

    builder.addAllQueries(transform(core.childQueries(), it -> it.convert(this)));
    ifNotNull(core.min, builder::setMinimum);

    return Query.newBuilder().setDisjunctionQuery(builder).build();
  }

  @Override
  public Query convert(CoreCustomQuery core) {
    throw unsupportedInProtostellar("custom search queries");
  }

  @Override
  public Query convert(CoreDateRangeQuery core) {
    DateRangeQuery.Builder builder = DateRangeQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);
    ifNotNull(core.field, builder::setField);

    ifNotNull(core.start, it -> {
      builder.setStartDate(it);
      if (core.inclusiveStart != null) {
        // Requires ING-381
        throw new FeatureNotAvailableException("inclusiveStart is not currently supported in DateRangeQuery for couchbase2");
      }
    });

    ifNotNull(core.end, it -> {
      builder.setEndDate(it);
      if (core.inclusiveEnd != null) {
        // Requires ING-381
        throw new FeatureNotAvailableException("inclusiveEnd is not currently supported in DateRangeQuery for couchbase2");
      }
    });

    ifNotNull(core.dateTimeParser, builder::setDateTimeParser);

    return Query.newBuilder().setDateRangeQuery(builder).build();
  }

  @Override
  public Query convert(CoreDocIdQuery core) {
    DocIdQuery.Builder builder = DocIdQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);

    builder.addAllIds(core.docIds);

    return Query.newBuilder().setDocIdQuery(builder).build();
  }

  @Override
  public Query convert(CoreGeoBoundingBoxQuery core) {
    GeoBoundingBoxQuery.Builder builder = GeoBoundingBoxQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);
    ifNotNull(core.field, builder::setField);

    builder.setTopLeft(toLatLng(core.topLeft));
    builder.setBottomRight(toLatLng(core.bottomRight));

    return Query.newBuilder().setGeoBoundingBoxQuery(builder).build();
  }

  @Override
  public Query convert(CoreGeoDistanceQuery core) {
    GeoDistanceQuery.Builder builder = GeoDistanceQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);
    ifNotNull(core.field, builder::setField);

    builder.setCenter(toLatLng(core.location));
    builder.setDistance(core.distance);

    return Query.newBuilder().setGeoDistanceQuery(builder).build();
  }

  @Override
  public Query convert(CoreGeoPolygonQuery core) {
    GeoPolygonQuery.Builder builder = GeoPolygonQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);
    ifNotNull(core.field, builder::setField);

    builder.addAllVertices(transform(core.coordinates, CoreProtostellarUtil::toLatLng));

    return Query.newBuilder().setGeoPolygonQuery(builder).build();
  }

  @Override
  public Query convert(CoreMatchAllQuery core) {
    return Query.newBuilder().setMatchAllQuery(MatchAllQuery.getDefaultInstance()).build();
  }

  @Override
  public Query convert(CoreMatchNoneQuery core) {
    return Query.newBuilder().setMatchNoneQuery(MatchNoneQuery.getDefaultInstance()).build();
  }

  @Override
  public Query convert(CoreMatchPhraseQuery core) {
    MatchPhraseQuery.Builder builder = MatchPhraseQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);
    ifNotNull(core.field, builder::setField);

    builder.setPhrase(core.matchPhrase);
    ifNotNull(core.analyzer, builder::setAnalyzer);

    return Query.newBuilder().setMatchPhraseQuery(builder).build();
  }

  @Override
  public Query convert(CoreMatchQuery core) {
    MatchQuery.Builder builder = MatchQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);
    ifNotNull(core.field, builder::setField);

    builder.setValue(core.match);
    ifNotNull(core.analyzer, builder::setAnalyzer);
    ifNotNull(core.fuzziness, it -> builder.setFuzziness(it));
    ifNotNull(core.prefixLength, it -> builder.setPrefixLength(it));
    ifNotNull(core.operator, it -> {
      MatchQuery.Operator protostellarOp = it == CoreMatchOperator.OR
        ? MatchQuery.Operator.OPERATOR_OR
        : MatchQuery.Operator.OPERATOR_AND;
      builder.setOperator(protostellarOp);
    });

    return Query.newBuilder().setMatchQuery(builder).build();
  }

  @Override
  public Query convert(CoreNumericRangeQuery core) {
    NumericRangeQuery.Builder builder = NumericRangeQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);
    ifNotNull(core.field, builder::setField);

    ifNotNull(core.min, min -> {
      builder.setMin(min.floatValue());
      ifNotNull(core.inclusiveMin, builder::setInclusiveMin);
    });

    ifNotNull(core.max, max -> {
      builder.setMax(max.floatValue());
      ifNotNull(core.inclusiveMax, builder::setInclusiveMax);
    });

    return Query.newBuilder().setNumericRangeQuery(builder).build();
  }

  @Override
  public Query convert(CorePhraseQuery core) {
    PhraseQuery.Builder builder = PhraseQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);
    ifNotNull(core.field, builder::setField);

    builder.addAllTerms(core.terms);

    return Query.newBuilder().setPhraseQuery(builder).build();
  }

  @Override
  public Query convert(CorePrefixQuery core) {
    PrefixQuery.Builder builder = PrefixQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);
    ifNotNull(core.field, builder::setField);

    builder.setPrefix(core.prefix);

    return Query.newBuilder().setPrefixQuery(builder).build();
  }

  @Override
  public Query convert(CoreQueryStringQuery core) {
    QueryStringQuery.Builder builder = QueryStringQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);

    builder.setQueryString(core.query);

    return Query.newBuilder().setQueryStringQuery(builder).build();

  }

  @Override
  public Query convert(CoreRegexpQuery core) {
    RegexpQuery.Builder builder = RegexpQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);
    ifNotNull(core.field, builder::setField);

    builder.setRegexp(core.regexp);

    return Query.newBuilder().setRegexpQuery(builder).build();
  }

  @Override
  public Query convert(CoreTermQuery core) {
    TermQuery.Builder builder = TermQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);
    ifNotNull(core.field, builder::setField);

    builder.setTerm(core.term);
    ifNotNull(core.prefixLength, it -> builder.setPrefixLength(it));
    ifNotNull(core.fuzziness, it -> builder.setFuzziness(it));

    return Query.newBuilder().setTermQuery(builder).build();
  }

  @Override
  public Query convert(CoreTermRangeQuery core) {
    TermRangeQuery.Builder builder = TermRangeQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);
    ifNotNull(core.field, builder::setField);

    ifNotNull(core.min, min -> {
      builder.setMin(min);
      ifNotNull(core.inclusiveMin, builder::setInclusiveMin);
    });

    ifNotNull(core.max, max -> {
      builder.setMax(max);
      ifNotNull(core.inclusiveMax, builder::setInclusiveMax);
    });

    return Query.newBuilder().setTermRangeQuery(builder).build();
  }

  @Override
  public Query convert(CoreWildcardQuery core) {
    WildcardQuery.Builder builder = WildcardQuery.newBuilder();
    ifNotNull(boostAsFloat(core), builder::setBoost);
    ifNotNull(core.field, builder::setField);

    builder.setWildcard(core.wildcard);

    return Query.newBuilder().setWildcardQuery(builder).build();
  }
}

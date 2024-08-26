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

package com.couchbase.search;

// [skip:<3.4.5]

import com.couchbase.JavaSdkCommandExecutor;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.search.AllowQueryingSearchIndexOptions;
import com.couchbase.client.java.manager.search.AnalyzeDocumentOptions;
import com.couchbase.client.java.manager.search.DisallowQueryingSearchIndexOptions;
import com.couchbase.client.java.manager.search.DropSearchIndexOptions;
import com.couchbase.client.java.manager.search.FreezePlanSearchIndexOptions;
import com.couchbase.client.java.manager.search.GetAllSearchIndexesOptions;
import com.couchbase.client.java.manager.search.GetIndexedSearchIndexOptions;
import com.couchbase.client.java.manager.search.GetSearchIndexOptions;
import com.couchbase.client.java.manager.search.PauseIngestSearchIndexOptions;
import com.couchbase.client.java.manager.search.ResumeIngestSearchIndexOptions;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.manager.search.UnfreezePlanSearchIndexOptions;
import com.couchbase.client.java.manager.search.UpsertSearchIndexOptions;
import com.couchbase.client.java.search.HighlightStyle;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.SearchScanConsistency;
import com.couchbase.client.java.search.facet.DateRange;
import com.couchbase.client.java.search.facet.NumericRange;
import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.queries.MatchOperator;
import com.couchbase.client.java.search.result.ReactiveSearchResult;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.search.sort.SearchFieldMissing;
import com.couchbase.client.java.search.sort.SearchFieldMode;
import com.couchbase.client.java.search.sort.SearchFieldType;
import com.couchbase.client.java.search.sort.SearchGeoDistanceUnits;
import com.couchbase.client.java.search.sort.SearchSort;
// [if:3.6.0]
import com.couchbase.client.java.search.vector.VectorQuery;
import com.couchbase.client.java.search.vector.VectorQueryCombination;
import com.couchbase.client.java.search.vector.VectorSearch;
import com.couchbase.client.java.search.vector.VectorSearchOptions;
import com.couchbase.client.java.search.SearchRequest;
// [end]
import com.couchbase.client.java.util.Coordinate;
import com.couchbase.client.performer.core.perf.PerRun;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.search.indexmanager.AllowQuerying;
import com.couchbase.client.protocol.sdk.search.indexmanager.AnalyzeDocument;
import com.couchbase.client.protocol.sdk.search.indexmanager.AnalyzeDocumentResult;
import com.couchbase.client.protocol.sdk.search.indexmanager.DisallowQuerying;
import com.couchbase.client.protocol.sdk.search.indexmanager.DropIndex;
import com.couchbase.client.protocol.sdk.search.indexmanager.FreezePlan;
import com.couchbase.client.protocol.sdk.search.indexmanager.GetAllIndexes;
import com.couchbase.client.protocol.sdk.search.indexmanager.GetIndex;
import com.couchbase.client.protocol.sdk.search.indexmanager.GetIndexedDocumentsCount;
import com.couchbase.client.protocol.sdk.search.indexmanager.PauseIngest;
import com.couchbase.client.protocol.sdk.search.indexmanager.ResumeIngest;
import com.couchbase.client.protocol.sdk.search.indexmanager.SearchIndexes;
import com.couchbase.client.protocol.sdk.search.indexmanager.UnfreezePlan;
import com.couchbase.client.protocol.sdk.search.indexmanager.UpsertIndex;
import com.couchbase.client.protocol.sdk.search.BlockingSearchResult;
import com.couchbase.client.protocol.sdk.search.Location;
import com.couchbase.client.protocol.sdk.search.SearchFacetResult;
import com.couchbase.client.protocol.sdk.search.SearchFacets;
import com.couchbase.client.protocol.sdk.search.SearchFragments;
import com.couchbase.client.protocol.sdk.search.SearchMetaData;
import com.couchbase.client.protocol.sdk.search.SearchMetrics;
import com.couchbase.client.protocol.sdk.search.SearchRowLocation;
import com.couchbase.client.protocol.shared.ContentAs;
import com.couchbase.stream.ReactiveSearchResultStreamer;
import com.couchbase.utils.ContentAsUtil;
import com.couchbase.utils.CustomJsonSerializer;
import com.google.common.primitives.Floats;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.JavaSdkCommandExecutor.convertMutationState;
import static com.couchbase.JavaSdkCommandExecutor.setSuccess;
import static com.couchbase.client.performer.core.util.TimeUtil.getTimeNow;
import static com.couchbase.client.protocol.sdk.search.MatchOperator.SEARCH_MATCH_OPERATOR_AND;
import static com.couchbase.client.protocol.sdk.search.MatchOperator.SEARCH_MATCH_OPERATOR_OR;
import static com.couchbase.client.protocol.sdk.search.SearchScanConsistency.SEARCH_SCAN_CONSISTENCY_NOT_BOUNDED;
import static com.couchbase.client.protocol.streams.Type.STREAM_FULL_TEXT_SEARCH;

public class SearchHelper {
  private SearchHelper() {
  }

  @Nullable
  static SearchOptions convertSearchOptions(boolean hasOptions, com.couchbase.client.protocol.sdk.search.SearchOptions o, ConcurrentHashMap<String, RequestSpan> spans) {
    if (!hasOptions) {
      return null;
    }

    var opts = SearchOptions.searchOptions();

    if (o.hasLimit()) {
      opts.limit(o.getLimit());
    }
    if (o.hasSkip()) {
      opts.skip(o.getSkip());
    }
    if (o.hasExplain()) {
      opts.explain(o.getExplain());
    }
    if (o.hasHighlight()) {
      var hasStyle = o.getHighlight().hasStyle();
      var hasFields = o.getHighlight().getFieldsCount() > 0;
      var style = switch (o.getHighlight().getStyle()) {
        case HIGHLIGHT_STYLE_ANSI -> HighlightStyle.ANSI;
        case HIGHLIGHT_STYLE_HTML -> HighlightStyle.HTML;
        default -> null;
      };
      if (hasStyle && hasFields) {
        opts.highlight(style, o.getHighlight().getFieldsList().toArray(new String[0]));
      } else if (hasStyle) {
        opts.highlight(style);
      } else if (hasFields) {
        opts.highlight(o.getHighlight().getFieldsList().toArray(new String[0]));
      }
    }
    if (o.getFieldsCount() > 0) {
      opts.fields(o.getFieldsList().toArray(new String[0]));
    }
    if (o.hasScanConsistency()) {
      if (o.getScanConsistency() == SEARCH_SCAN_CONSISTENCY_NOT_BOUNDED) {
        opts.scanConsistency(SearchScanConsistency.NOT_BOUNDED);
      } else {
        throw new UnsupportedOperationException();
      }
    }
    if (o.hasConsistentWith()) {
      opts.consistentWith(convertMutationState(o.getConsistentWith()));
    }
    if (o.getSortCount() > 0) {
      opts.sort(o.getSortList()
              .stream()
              .map(sort -> {
                Object out;

                if (sort.hasField()) {
                  var ss = sort.getField();
                  var s = SearchSort.byField(ss.getField());
                  out = s;
                  if (ss.hasDesc()) {
                    s.desc(ss.getDesc());
                  }
                  if (ss.hasType()) {
                    s.type(switch (ss.getType()) {
                      case "auto" -> SearchFieldType.AUTO;
                      case "date" -> SearchFieldType.DATE;
                      case "number" -> SearchFieldType.NUMBER;
                      case "string" -> SearchFieldType.STRING;
                      default -> throw new UnsupportedOperationException();
                    });
                  }
                  if (ss.hasMode()) {
                    s.mode(switch (ss.getMode()) {
                      case "default" -> SearchFieldMode.DEFAULT;
                      case "min" -> SearchFieldMode.MIN;
                      case "max" -> SearchFieldMode.MAX;
                      default -> throw new UnsupportedOperationException();
                    });
                  }
                  if (ss.hasMissing()) {
                    s.missing(switch (ss.getMissing()) {
                      case "first" -> SearchFieldMissing.FIRST;
                      case "last" -> SearchFieldMissing.LAST;
                      default -> throw new UnsupportedOperationException();
                    });
                  }
                } else if (sort.hasScore()) {
                  var ss = sort.getScore();
                  var s = SearchSort.byScore();
                  out = s;
                  if (ss.hasDesc()) {
                    s.desc(ss.getDesc());
                  }
                } else if (sort.hasId()) {
                  var ss = sort.getId();
                  var s = SearchSort.byId();
                  out = s;
                  if (ss.hasDesc()) {
                    s.desc(ss.getDesc());
                  }
                } else if (sort.hasGeoDistance()) {
                  var ss = sort.getGeoDistance();
                  var s = SearchSort.byGeoDistance(ss.getLocation().getLon(), ss.getLocation().getLat(), ss.getField());
                  out = s;
                  if (ss.hasDesc()) {
                    s.desc(ss.getDesc());
                  }
                  if (ss.hasUnit()) {
                    s.unit(switch (ss.getUnit()) {
                      case SEARCH_GEO_DISTANCE_UNITS_METERS -> SearchGeoDistanceUnits.Meters;
                      case SEARCH_GEO_DISTANCE_UNITS_MILES -> SearchGeoDistanceUnits.Miles;
                      case SEARCH_GEO_DISTANCE_UNITS_CENTIMETERS -> SearchGeoDistanceUnits.Centimeters;
                      case SEARCH_GEO_DISTANCE_UNITS_MILLIMETERS -> SearchGeoDistanceUnits.Millimeters;
                      case SEARCH_GEO_DISTANCE_UNITS_NAUTICAL_MILES -> SearchGeoDistanceUnits.NauticalMiles;
                      case SEARCH_GEO_DISTANCE_UNITS_KILOMETERS -> SearchGeoDistanceUnits.Kilometers;
                      case SEARCH_GEO_DISTANCE_UNITS_FEET -> SearchGeoDistanceUnits.Feet;
                      case SEARCH_GEO_DISTANCE_UNITS_YARDS -> SearchGeoDistanceUnits.Yards;
                      case SEARCH_GEO_DISTANCE_UNITS_INCHES -> SearchGeoDistanceUnits.Inch;
                      default -> throw new UnsupportedOperationException();
                    });
                  }
                } else if (sort.hasRaw()) {
                  out = sort.getRaw();
                } else {
                  throw new UnsupportedOperationException();
                }

                return out;
              }).toList()
              .toArray(new Object[0]));
    }
    if (o.getFacetsCount() > 0) {
      var facets = new HashMap<String, SearchFacet>();
      o.getFacetsMap().forEach((k, v) -> {
        SearchFacet facet;
        if (v.hasTerm()) {
          var f = v.getTerm();
          if (f.hasSize()) {
            facet = SearchFacet.term(f.getField(), f.getSize());
          } else {
            throw new UnsupportedOperationException("The Java SDK has size as a mandatory param for TermFacet");
          }
        } else if (v.hasNumericRange()) {
          var f = v.getNumericRange();
          if (f.hasSize()) {
            facet = SearchFacet.numericRange(f.getField(), f.getSize(), f.getNumericRangesList().stream()
                    .map(nr -> NumericRange.create(nr.getName(), (double) nr.getMin(), (double) nr.getMax()))
                    .toList());
          } else {
            throw new UnsupportedOperationException("The Java SDK has size as a mandatory param for NumericRangeFacet");
          }
        } else if (v.hasDateRange()) {
          var f = v.getDateRange();
          if (f.hasSize()) {
            facet = SearchFacet.dateRange(f.getField(), f.getSize(), f.getDateRangesList().stream()
                    .map(nr -> DateRange.create(nr.getName(), convertTimestamp(nr.getStart()), convertTimestamp(nr.getEnd())))
                    .toList());
          } else {
            throw new UnsupportedOperationException("The Java SDK has size as a mandatory param for NumericRangeFacet");
          }
        } else {
          throw new UnsupportedOperationException();
        }
        facets.put(k, facet);
      });
      opts.facets(facets);
    }
    if (o.hasTimeoutMillis()) {
      opts.timeout(Duration.ofMillis(o.getTimeoutMillis()));
    }
    if (o.hasParentSpanId()) {
      opts.parentSpan(spans.get(o.getParentSpanId()));
    }
    if (o.getRawCount() > 0) {
      o.getRawMap().forEach((k, v) -> opts.raw(k, v));
    }
    if (o.hasIncludeLocations()) {
      opts.includeLocations(o.getIncludeLocations());
    }

    if (o.hasSerialize()) {
      if (o.getSerialize().hasCustomSerializer() && o.getSerialize().getCustomSerializer()) {
        CustomJsonSerializer customSerializer = new CustomJsonSerializer();
        opts.serializer(customSerializer);
      }
    }

    return opts;
  }

  private static Instant convertTimestamp(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds());
  }

  private static List<SearchQuery> convertSearchQueries(List<com.couchbase.client.protocol.sdk.search.SearchQuery> queries) {
    return queries.stream()
            .map(query -> convertSearchQuery(query))
            .toList();
  }

  private static SearchQuery convertSearchQuery(com.couchbase.client.protocol.sdk.search.SearchQuery q) {
    SearchQuery query;

    switch (q.getQueryCase()) {
      case MATCH -> {
        var qr = q.getMatch();
        var out = SearchQuery.match(qr.getMatch());
        if (qr.hasField()) {
          out.field(qr.getField());
        }
        if (qr.hasAnalyzer()) {
          out.analyzer(qr.getAnalyzer());
        }
        if (qr.hasPrefixLength()) {
          out.prefixLength(qr.getPrefixLength());
        }
        if (qr.hasFuzziness()) {
          out.fuzziness(qr.getFuzziness());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        if (qr.hasOperator()) {
          if (qr.getOperator() == SEARCH_MATCH_OPERATOR_OR) {
            out.operator(MatchOperator.OR);
          } else if (qr.getOperator() == SEARCH_MATCH_OPERATOR_AND) {
            out.operator(MatchOperator.AND);
          } else {
            throw new UnsupportedOperationException();
          }
        }
        query = out;
      }
      case MATCH_PHRASE -> {
        var qr = q.getMatchPhrase();
        var out = SearchQuery.matchPhrase(qr.getMatchPhrase());
        if (qr.hasField()) {
          out.field(qr.getField());
        }
        if (qr.hasAnalyzer()) {
          out.analyzer(qr.getAnalyzer());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case REGEXP -> {
        var qr = q.getRegexp();
        var out = SearchQuery.regexp(qr.getRegexp());
        if (qr.hasField()) {
          out.field(qr.getField());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case QUERY_STRING -> {
        var qr = q.getQueryString();
        var out = SearchQuery.queryString(qr.getQuery());
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case WILDCARD -> {
        var qr = q.getWildcard();
        var out = SearchQuery.wildcard(qr.getWildcard());
        if (qr.hasField()) {
          out.field(qr.getField());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case DOC_ID -> {
        var qr = q.getDocId();
        var out = SearchQuery.docId(qr.getIdsList().toArray(new String[0]));
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case SEARCH_BOOLEAN_FIELD -> {
        var qr = q.getSearchBooleanField();
        var out = SearchQuery.booleanField(qr.getBool());
        if (qr.hasField()) {
          out.field(qr.getField());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case DATE_RANGE -> {
        var qr = q.getDateRange();
        var out = SearchQuery.dateRange();
        if (qr.hasStart()) {
          if (qr.hasInclusiveStart()) {
            out.start(qr.getStart(), qr.getInclusiveStart());
          } else {
            out.start(qr.getStart());
          }
        }
        if (qr.hasEnd()) {
          if (qr.hasInclusiveEnd()) {
            out.end(qr.getEnd(), qr.getInclusiveEnd());
          } else {
            out.end(qr.getEnd());
          }
        }
        if (qr.hasDatetimeParser()) {
          out.dateTimeParser(qr.getDatetimeParser());
        }
        if (qr.hasField()) {
          out.field(qr.getField());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case NUMERIC_RANGE -> {
        var qr = q.getNumericRange();
        var out = SearchQuery.numericRange();
        if (qr.hasMin()) {
          if (qr.hasInclusiveMin()) {
            out.min(qr.getMin(), qr.getInclusiveMin());
          } else {
            out.min(qr.getMin());
          }
        }
        if (qr.hasMax()) {
          if (qr.hasInclusiveMax()) {
            out.max(qr.getMax(), qr.getInclusiveMax());
          } else {
            out.max(qr.getMax());
          }
        }
        if (qr.hasField()) {
          out.field(qr.getField());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case TERM_RANGE -> {
        var qr = q.getTermRange();
        var out = SearchQuery.termRange();
        if (qr.hasMin()) {
          if (qr.hasInclusiveMin()) {
            out.min(qr.getMin(), qr.getInclusiveMin());
          } else {
            out.min(qr.getMin());
          }
        }
        if (qr.hasMax()) {
          if (qr.hasInclusiveMax()) {
            out.max(qr.getMax(), qr.getInclusiveMax());
          } else {
            out.max(qr.getMax());
          }
        }
        if (qr.hasField()) {
          out.field(qr.getField());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case GEO_DISTANCE -> {
        var qr = q.getGeoDistance();
        var coords = convertLocation(qr.getLocation());
        var out = SearchQuery.geoDistance(coords, qr.getDistance());
        if (qr.hasField()) {
          out.field(qr.getField());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case GEO_BOUNDING_BOX -> {
        var qr = q.getGeoBoundingBox();
        var topLeft = convertLocation(qr.getTopLeft());
        var bottomRight = convertLocation(qr.getBottomRight());
        var out = SearchQuery.geoBoundingBox(topLeft, bottomRight);
        if (qr.hasField()) {
          out.field(qr.getField());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case CONJUNCTION -> {
        var qr = q.getConjunction();
        var converted = convertSearchQueries(qr.getConjunctsList());
        var out = SearchQuery.conjuncts(converted.toArray(new SearchQuery[0]));
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case DISJUNCTION -> {
        var qr = q.getDisjunction();
        var converted = convertSearchQueries(qr.getDisjunctsList());
        var out = SearchQuery.disjuncts(converted.toArray(new SearchQuery[0]));
        if (qr.hasMin()) {
          out.min(qr.getMin());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case BOOLEAN -> {
        var qr = q.getBoolean();
        var out = SearchQuery.booleans();

        if (qr.getMustCount() > 0) {
          var converted = convertSearchQueries(qr.getMustList());
          out.must(converted.toArray(new SearchQuery[0]));
        }
        if (qr.getShouldCount() > 0) {
          var converted = convertSearchQueries(qr.getShouldList());
          out.should(converted.toArray(new SearchQuery[0]));
        }
        if (qr.getMustNotCount() > 0) {
          var converted = convertSearchQueries(qr.getMustList());
          out.mustNot(converted.toArray(new SearchQuery[0]));
        }
        if (qr.hasShouldMin()) {
          out.shouldMin(qr.getShouldMin());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;

      }
      case TERM -> {
        var qr = q.getTerm();
        var out = SearchQuery.term(qr.getTerm());
        if (qr.hasField()) {
          out.field(qr.getField());
        }
        if (qr.hasFuzziness()) {
          out.fuzziness(qr.getFuzziness());
        }
        if (qr.hasPrefixLength()) {
          out.prefixLength(qr.getPrefixLength());
        }
        if (qr.hasField()) {
          out.field(qr.getField());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case PREFIX -> {
        var qr = q.getPrefix();
        var out = SearchQuery.prefix(qr.getPrefix());
        if (qr.hasField()) {
          out.field(qr.getField());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case PHRASE -> {
        var qr = q.getPhrase();
        var out = SearchQuery.phrase(qr.getTermsList().toArray(new String[0]));
        if (qr.hasField()) {
          out.field(qr.getField());
        }
        if (qr.hasBoost()) {
          out.boost(qr.getBoost());
        }
        query = out;
      }
      case MATCH_ALL -> {
        query = SearchQuery.matchAll();
      }
      case MATCH_NONE -> {
        query = SearchQuery.matchNone();
      }
      default -> {
        throw new UnsupportedOperationException();
      }
    }

    return query;
  }

  private static Coordinate convertLocation(Location location) {
    return Coordinate.ofLonLat(location.getLon(), location.getLat());
  }

  public static Result handleSearchQueryBlocking(Cluster cluster,
                                                 @Nullable Scope scope,
                                                 ConcurrentHashMap<String, RequestSpan> spans,
                                                 com.couchbase.client.protocol.sdk.search.Search command,
                                                 com.couchbase.client.protocol.sdk.Command op
                                                 ) {
    var q = command.getQuery();

    var query = SearchHelper.convertSearchQuery(q);
    var options = SearchHelper.convertSearchOptions(command.hasOptions(), command.getOptions(), spans);

    var result = Result.newBuilder();
    result.setInitiated(getTimeNow());
    long start = System.nanoTime();

    SearchResult r;
    if (scope == null) {
      if (options != null) {
        r = cluster.searchQuery(command.getIndexName(), query, options);
      } else {
        r = cluster.searchQuery(command.getIndexName(), query);
      }
    } else {

      if (options != null) {
        r = scope.searchQuery(command.getIndexName(), query, options);
      } else {
        r = scope.searchQuery(command.getIndexName(), query);
      }
    }

    result.setElapsedNanos(System.nanoTime() - start);

    if(op.getReturnResult())  result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
            .setSearchBlockingResult(convertResult(r, command.hasFieldsAs() ? command.getFieldsAs() : null)));
    else setSuccess(result);

    return result.build();
  }

  public static Mono<Void> handleSearchQueryReactive(Cluster cluster,
                                                     @Nullable Scope scope,
                                                     ConcurrentHashMap<String, RequestSpan> spans,
                                                     com.couchbase.client.protocol.sdk.search.Search command,
                                                     PerRun perRun) {
    return Mono.defer(() -> {
      var q = command.getQuery();

      var query = SearchHelper.convertSearchQuery(q);
      var options = SearchHelper.convertSearchOptions(command.hasOptions(), command.getOptions(), spans);

      var result = Result.newBuilder();
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();

      Mono<ReactiveSearchResult> r;
      if (scope == null) {
        if (options != null) {
          r = cluster.reactive().searchQuery(command.getIndexName(), query, options);
        } else {
          r = cluster.reactive().searchQuery(command.getIndexName(), query);
        }
      } else {
        if (options != null) {
          r = scope.reactive().searchQuery(command.getIndexName(), query, options);
        } else {
          r = scope.reactive().searchQuery(command.getIndexName(), query);
        }
      }

      return r.doOnNext(re -> {
        result.setElapsedNanos(System.nanoTime() - start);

        var streamer = new ReactiveSearchResultStreamer(re,
                perRun,
                command.getStreamConfig().getStreamId(),
                command.getStreamConfig(),
                command.hasFieldsAs() ? command.getFieldsAs() : null,
                JavaSdkCommandExecutor::convertExceptionShared);
        result.setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                .setCreated(com.couchbase.client.protocol.streams.Created.newBuilder()
                        .setType(STREAM_FULL_TEXT_SEARCH)
                        .setStreamId(streamer.streamId())));
        perRun.resultsStream().enqueue(result.build());
        perRun.streamerOwner().addAndStart(streamer);
      });
    }).then();
  }

  // [if:3.6.0]
  public static Result handleSearchBlocking(Cluster cluster,
                                            @Nullable Scope scope,
                                            ConcurrentHashMap<String, RequestSpan> spans,
                                            com.couchbase.client.protocol.sdk.search.SearchWrapper command,
                                            com.couchbase.client.protocol.sdk.Command op) {
    var search = command.getSearch();
    var request = SearchHelper.convertSearchRequest(search.getRequest());
    var options = SearchHelper.convertSearchOptions(search.hasOptions(), search.getOptions(), spans);

    var result = Result.newBuilder();
    result.setInitiated(getTimeNow());
    long start = System.nanoTime();

    SearchResult r;
    if (scope == null) {
      if (options != null) {
        r = cluster.search(search.getIndexName(), request, options);
      } else {
        r = cluster.search(search.getIndexName(), request);
      }
    } else {
      if (options != null) {
        r = scope.search(search.getIndexName(), request, options);
      } else {
        r = scope.search(search.getIndexName(), request);
      }
    }

    result.setElapsedNanos(System.nanoTime() - start);

    if(op.getReturnResult())  result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
            .setSearchBlockingResult(convertResult(r, command.hasFieldsAs() ? command.getFieldsAs() : null)));
    else setSuccess(result);

    return result.build();
  }

  public static Mono<Void> handleSearchReactive(Cluster cluster,
                                                @Nullable Scope scope,
                                                ConcurrentHashMap<String, RequestSpan> spans,
                                                com.couchbase.client.protocol.sdk.search.SearchWrapper command,
                                                PerRun perRun) {
    return Mono.defer(() -> {
      var search = command.getSearch();
      var request = SearchHelper.convertSearchRequest(search.getRequest());
      var options = SearchHelper.convertSearchOptions(search.hasOptions(), search.getOptions(), spans);

      var result = Result.newBuilder();
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();

      Mono<ReactiveSearchResult> r;
      if (scope == null) {
        if (options != null) {
          r = cluster.reactive().search(search.getIndexName(), request, options);
        } else {
          r = cluster.reactive().search(search.getIndexName(), request);
        }
      } else {
        if (options != null) {
          r = scope.reactive().search(search.getIndexName(), request, options);
        } else {
          r = scope.reactive().search(search.getIndexName(), request);
        }
      }

      return r.doOnNext(re -> {
        result.setElapsedNanos(System.nanoTime() - start);

        var streamer = new ReactiveSearchResultStreamer(re,
                perRun,
                command.getStreamConfig().getStreamId(),
                command.getStreamConfig(),
                command.hasFieldsAs() ? command.getFieldsAs() : null,
                JavaSdkCommandExecutor::convertExceptionShared);
        result.setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                .setCreated(com.couchbase.client.protocol.streams.Created.newBuilder()
                        .setType(STREAM_FULL_TEXT_SEARCH)
                        .setStreamId(streamer.streamId())));
        perRun.resultsStream().enqueue(result.build());
        perRun.streamerOwner().addAndStart(streamer);
      });
    }).then();
  }
  // [end]

  private static BlockingSearchResult convertResult(SearchResult result, @Nullable ContentAs fieldsAs) {
    return BlockingSearchResult.newBuilder()
            .addAllRows(result.rows().stream().map(v -> convertRow(v, fieldsAs)).toList())
            .setMetaData(convertMetaData(result.metaData()))
            .setFacets(convertFacets(result.facets()))
            .build();
  }

  public static SearchFacets convertFacets(Map<String, com.couchbase.client.java.search.result.SearchFacetResult> facets) {
    var out = new HashMap<String, SearchFacetResult>();

    facets.forEach((k, v) -> {
      out.put(k, SearchFacetResult.newBuilder()
              .setName(v.name())
              .setField(v.field())
              .setTotal(v.total())
              .setMissing(v.missing())
              .setOther(v.other())
              .build());
    });

    return SearchFacets.newBuilder()
            .putAllFacets(out)
            .build();
  }

  public static SearchMetaData convertMetaData(com.couchbase.client.java.search.SearchMetaData metaData) {
    return SearchMetaData.newBuilder()
            .setMetrics(SearchMetrics.newBuilder()
                    .setTookMsec(metaData.metrics().took().toMillis())
                    .setTotalRows(metaData.metrics().totalRows())
                    .setMaxScore(metaData.metrics().maxScore())
                    .setTotalPartitionCount(metaData.metrics().totalPartitionCount())
                    .setSuccessPartitionCount(metaData.metrics().successPartitionCount())
                    .setErrorPartitionCount(metaData.metrics().errorPartitionCount()))
            .putAllErrors(metaData.errors())
            .build();
  }

  public static com.couchbase.client.protocol.sdk.search.SearchRow convertRow(SearchRow v, @Nullable ContentAs fieldsAs) {
    var builder = com.couchbase.client.protocol.sdk.search.SearchRow.newBuilder()
            .setIndex(v.index())
            .setId(v.id())
            .setScore(v.score())
            .setExplanation(ByteString.copyFrom(v.explanation().toBytes()));


    var fragments = new HashMap<String, SearchFragments>();
    v.fragments().forEach((k, x) -> {
      fragments.put(k, SearchFragments.newBuilder().addAllFragments(x).build());
    });
    builder.putAllFragments(fragments);

    v.locations().ifPresent(locations -> {
      builder.addAllLocations(locations.getAll().stream()
              .map(l -> {
                List<Integer> positions = l.arrayPositions() == null ? Collections.emptyList()
                        : Arrays.stream(l.arrayPositions())
                        .boxed()
                        .map(Long::intValue)
                        .toList();
                return SearchRowLocation.newBuilder()
                        .setField(l.field())
                        .setTerm(l.term())
                        .setPosition((int) l.pos())
                        .setStart((int) l.start())
                        .setEnd((int) l.end())
                        .addAllArrayPositions(positions)
                        .build();
              })
              .toList());
    });

    if (fieldsAs != null) {
      var content = ContentAsUtil.contentType(fieldsAs,
              () -> v.fieldsAs(byte[].class),
              () -> v.fieldsAs(String.class),
              () -> v.fieldsAs(JsonObject.class),
              () -> v.fieldsAs(JsonArray.class),
              () -> v.fieldsAs(Boolean.class),
              () -> v.fieldsAs(Integer.class),
              () -> v.fieldsAs(Double.class));
      if (content.isFailure()) {
        throw content.exception();
      }
      builder.setFields(content.value());
    }

    return builder.build();
  }

  public static Result handleClusterSearchIndexManager(Cluster cluster,
                                                       ConcurrentHashMap<String, RequestSpan> spans,
                                                       com.couchbase.client.protocol.sdk.Command command) {
    var sim = command.getClusterCommand().getSearchIndexManager();

    if (!sim.hasShared()) {
      throw new UnsupportedOperationException();
    }

    return handleSearchIndexManagerBlockingShared(cluster, null, spans, command, sim.getShared());
  }

  public static Result handleScopeSearchIndexManager(Scope scope,
                                                     ConcurrentHashMap<String, RequestSpan> spans,
                                                     com.couchbase.client.protocol.sdk.Command command) {
    var sim = command.getScopeCommand().getSearchIndexManager();

    if (!sim.hasShared()) {
      throw new UnsupportedOperationException();
    }

    return handleSearchIndexManagerBlockingShared(null, scope, spans, command, sim.getShared());
  }

  /*
   * If used for testing cluster.searchIndexes(), cluster will be non-null.
   * If used for testing scope.searchIndexes(), scope will be non-null.
   */
  private static Result handleSearchIndexManagerBlockingShared(@Nullable Cluster cluster,
                                                               @Nullable Scope scope,
                                                               ConcurrentHashMap<String, RequestSpan> spans,
                                                               com.couchbase.client.protocol.sdk.Command op,
                                                               com.couchbase.client.protocol.sdk.search.indexmanager.Command command) {
    var result = Result.newBuilder();

    if (command.hasGetIndex()) {
      var request = command.getGetIndex();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      SearchIndex index = null;
      if (scope == null) {
        if (options == null) {
          index = cluster.searchIndexes().getIndex(request.getIndexName());
        } else {
          index = cluster.searchIndexes().getIndex(request.getIndexName(), options);
        }
      } else {
        if (options == null) {
          index = scope.searchIndexes().getIndex(request.getIndexName());
        } else {
          index = scope.searchIndexes().getIndex(request.getIndexName(), options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      if (op.getReturnResult()) {
        populateResult(result, index);
      } else {
        setSuccess(result);
      }
    } else if (command.hasGetAllIndexes()) {
      var request = command.getGetAllIndexes();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      List<SearchIndex> indexes;
      if (scope == null) {
        if (options == null) {
          indexes = cluster.searchIndexes().getAllIndexes();
        } else {
          indexes = cluster.searchIndexes().getAllIndexes(options);
        }
      } else {
        if (options == null) {
          indexes = scope.searchIndexes().getAllIndexes();
        } else {
          indexes = scope.searchIndexes().getAllIndexes(options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      if (op.getReturnResult()) {
        populateResult(result, indexes);
      } else {
        setSuccess(result);
      }
    } else if (command.hasUpsertIndex()) {
      var request = command.getUpsertIndex();
      var options = createOptions(request, spans);
      var converted = SearchIndex.fromJson(request.getIndexDefinition().toStringUtf8());
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (scope == null) {
        if (options == null) {
          cluster.searchIndexes().upsertIndex(converted);
        } else {
          cluster.searchIndexes().upsertIndex(converted, options);
        }
      } else {
        if (options == null) {
          scope.searchIndexes().upsertIndex(converted);
        } else {
          scope.searchIndexes().upsertIndex(converted, options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    } else if (command.hasDropIndex()) {
      var request = command.getDropIndex();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (scope == null) {
        if (options == null) {
          cluster.searchIndexes().dropIndex(request.getIndexName());
        } else {
          cluster.searchIndexes().dropIndex(request.getIndexName(), options);
        }
      } else {
        if (options == null) {
          scope.searchIndexes().dropIndex(request.getIndexName());
        } else {
          scope.searchIndexes().dropIndex(request.getIndexName(), options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    } else if (command.hasPauseIngest()) {
      var request = command.getPauseIngest();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (scope == null) {
        if (options == null) {
          cluster.searchIndexes().pauseIngest(request.getIndexName());
        } else {
          cluster.searchIndexes().pauseIngest(request.getIndexName(), options);
        }
      } else {
        if (options == null) {
          scope.searchIndexes().pauseIngest(request.getIndexName());
        } else {
          scope.searchIndexes().pauseIngest(request.getIndexName(), options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    } else if (command.hasResumeIngest()) {
      var request = command.getResumeIngest();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (scope == null) {
        if (options == null) {
          cluster.searchIndexes().resumeIngest(request.getIndexName());
        } else {
          cluster.searchIndexes().resumeIngest(request.getIndexName(), options);
        }
      } else {
        if (options == null) {
          scope.searchIndexes().resumeIngest(request.getIndexName());
        } else {
          scope.searchIndexes().resumeIngest(request.getIndexName(), options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    } else if (command.hasAllowQuerying()) {
      var request = command.getAllowQuerying();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (scope == null) {
        if (options == null) {
          cluster.searchIndexes().allowQuerying(request.getIndexName());
        } else {
          cluster.searchIndexes().allowQuerying(request.getIndexName(), options);
        }
      } else {
        if (options == null) {
          scope.searchIndexes().allowQuerying(request.getIndexName());
        } else {
          scope.searchIndexes().allowQuerying(request.getIndexName(), options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    } else if (command.hasDisallowQuerying()) {
      var request = command.getDisallowQuerying();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (scope == null) {
        if (options == null) {
          cluster.searchIndexes().disallowQuerying(request.getIndexName());
        } else {
          cluster.searchIndexes().disallowQuerying(request.getIndexName(), options);
        }
      } else {
        if (options == null) {
          scope.searchIndexes().disallowQuerying(request.getIndexName());
        } else {
          scope.searchIndexes().disallowQuerying(request.getIndexName(), options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    } else if (command.hasFreezePlan()) {
      var request = command.getFreezePlan();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (scope == null) {
        if (options == null) {
          cluster.searchIndexes().freezePlan(request.getIndexName());
        } else {
          cluster.searchIndexes().freezePlan(request.getIndexName(), options);
        }
      } else {
        if (options == null) {
          scope.searchIndexes().freezePlan(request.getIndexName());
        } else {
          scope.searchIndexes().freezePlan(request.getIndexName(), options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    } else if (command.hasUnfreezePlan()) {
      var request = command.getUnfreezePlan();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      if (scope == null) {
        if (options == null) {
          cluster.searchIndexes().unfreezePlan(request.getIndexName());
        } else {
          cluster.searchIndexes().unfreezePlan(request.getIndexName(), options);
        }
      } else {
        if (options == null) {
          scope.searchIndexes().unfreezePlan(request.getIndexName());
        } else {
          scope.searchIndexes().unfreezePlan(request.getIndexName(), options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      setSuccess(result);
    } else if (command.hasGetIndexedDocumentsCount()) {
      var request = command.getGetIndexedDocumentsCount();
      var options = createOptions(request, spans);
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      long count = 0;
      if (scope == null) {
        if (options == null) {
          count = cluster.searchIndexes().getIndexedDocumentsCount(request.getIndexName());
        } else {
          count = cluster.searchIndexes().getIndexedDocumentsCount(request.getIndexName(), options);
        }
      } else {
        if (options == null) {
          count = scope.searchIndexes().getIndexedDocumentsCount(request.getIndexName());
        } else {
          count = scope.searchIndexes().getIndexedDocumentsCount(request.getIndexName(), options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      if (op.getReturnResult()) {
        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setSearchIndexManagerResult(com.couchbase.client.protocol.sdk.search.indexmanager.Result.newBuilder()
                        .setIndexedDocumentCounts((int) count)));
      } else {
        setSuccess(result);
      }
    } else if (command.hasAnalyzeDocument()) {
      var request = command.getAnalyzeDocument();
      var options = createOptions(request, spans);
      var document = JsonObject.fromJson(request.getDocument().toString());
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();
      List<JsonObject> results = null;
      if (scope == null) {
        if (options == null) {
          results = cluster.searchIndexes().analyzeDocument(request.getIndexName(), document);
        } else {
          results = cluster.searchIndexes().analyzeDocument(request.getIndexName(), document, options);
        }
      } else {
        if (options == null) {
          results = scope.searchIndexes().analyzeDocument(request.getIndexName(), document);
        } else {
          results = scope.searchIndexes().analyzeDocument(request.getIndexName(), document, options);
        }
      }
      result.setElapsedNanos(System.nanoTime() - start);
      if (op.getReturnResult()) {
        populateAnalyzeDocumentResult(result, results);
      } else {
        setSuccess(result);
      }
    }

    return result.build();
  }

  private static void populateAnalyzeDocumentResult(Result.Builder result, List<JsonObject> results) {
    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
            .setSearchIndexManagerResult(com.couchbase.client.protocol.sdk.search.indexmanager.Result.newBuilder()
                    .setAnalyzeDocument(AnalyzeDocumentResult.newBuilder()
                            .addAllResults(results.stream().map(v -> ByteString.copyFrom(v.toBytes())).toList())
                            .build())));
  }

  private static void populateResult(Result.Builder result, List<SearchIndex> indexes) {
    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
            .setSearchIndexManagerResult(com.couchbase.client.protocol.sdk.search.indexmanager.Result.newBuilder()
                    .setIndexes(SearchIndexes.newBuilder()
                            .addAllIndexes(indexes.stream().map(SearchHelper::convertSearchIndex).toList())
                            .build())));
  }

  private static void populateResult(Result.Builder result, SearchIndex index) {
    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
            .setSearchIndexManagerResult(com.couchbase.client.protocol.sdk.search.indexmanager.Result.newBuilder()
                    .setIndex(convertSearchIndex(index))));
  }

  private static com.couchbase.client.protocol.sdk.search.indexmanager.SearchIndex convertSearchIndex(SearchIndex i) {
    return com.couchbase.client.protocol.sdk.search.indexmanager.SearchIndex.newBuilder()
            .setUuid(i.uuid())
            .setName(i.name())
            .setType(i.type())
            .setParams(ByteString.copyFrom(JsonObject.from(i.params()).toBytes()))
            .setSourceParams(ByteString.copyFrom(JsonObject.from(i.sourceParams()).toBytes()))
            .setPlanParams(ByteString.copyFrom(JsonObject.from(i.planParams()).toBytes()))
            .setSourceUuid(i.sourceUuid())
            .setSourceType(i.sourceType())
            .build();
  }

  private @Nullable
  static GetSearchIndexOptions createOptions(GetIndex request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (!request.hasOptions()) {
      return null;
    }

    var out = GetSearchIndexOptions.getSearchIndexOptions();
    var o = request.getOptions();

    if (o.hasTimeoutMsecs()) {
      out.timeout(Duration.ofMinutes(o.getTimeoutMsecs()));
    }
    if (o.hasParentSpanId()) {
      out.parentSpan(spans.get(o.getParentSpanId()));
    }

    return out;
  }

  private @Nullable
  static GetAllSearchIndexesOptions createOptions(GetAllIndexes request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (!request.hasOptions()) {
      return null;
    }

    var out = GetAllSearchIndexesOptions.getAllSearchIndexesOptions();
    var o = request.getOptions();

    if (o.hasTimeoutMsecs()) {
      out.timeout(Duration.ofMinutes(o.getTimeoutMsecs()));
    }
    if (o.hasParentSpanId()) {
      out.parentSpan(spans.get(o.getParentSpanId()));
    }

    return out;
  }

  private @Nullable
  static UpsertSearchIndexOptions createOptions(UpsertIndex request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (!request.hasOptions()) {
      return null;
    }

    var out = UpsertSearchIndexOptions.upsertSearchIndexOptions();
    var o = request.getOptions();

    if (o.hasTimeoutMsecs()) {
      out.timeout(Duration.ofMinutes(o.getTimeoutMsecs()));
    }
    if (o.hasParentSpanId()) {
      out.parentSpan(spans.get(o.getParentSpanId()));
    }

    return out;
  }

  private @Nullable
  static DropSearchIndexOptions createOptions(DropIndex request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (!request.hasOptions()) {
      return null;
    }

    var out = DropSearchIndexOptions.dropSearchIndexOptions();
    var o = request.getOptions();

    if (o.hasTimeoutMsecs()) {
      out.timeout(Duration.ofMinutes(o.getTimeoutMsecs()));
    }
    if (o.hasParentSpanId()) {
      out.parentSpan(spans.get(o.getParentSpanId()));
    }

    return out;
  }

  private @Nullable
  static GetIndexedSearchIndexOptions createOptions(GetIndexedDocumentsCount request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (!request.hasOptions()) {
      return null;
    }

    var out = GetIndexedSearchIndexOptions.getIndexedSearchIndexOptions();
    var o = request.getOptions();

    if (o.hasTimeoutMsecs()) {
      out.timeout(Duration.ofMinutes(o.getTimeoutMsecs()));
    }
    if (o.hasParentSpanId()) {
      out.parentSpan(spans.get(o.getParentSpanId()));
    }

    return out;
  }

  private @Nullable
  static PauseIngestSearchIndexOptions createOptions(PauseIngest request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (!request.hasOptions()) {
      return null;
    }

    var out = PauseIngestSearchIndexOptions.pauseIngestSearchIndexOptions();
    var o = request.getOptions();

    if (o.hasTimeoutMsecs()) {
      out.timeout(Duration.ofMinutes(o.getTimeoutMsecs()));
    }
    if (o.hasParentSpanId()) {
      out.parentSpan(spans.get(o.getParentSpanId()));
    }

    return out;
  }

  private @Nullable
  static ResumeIngestSearchIndexOptions createOptions(ResumeIngest request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (!request.hasOptions()) {
      return null;
    }

    var out = ResumeIngestSearchIndexOptions.resumeIngestSearchIndexOptions();
    var o = request.getOptions();

    if (o.hasTimeoutMsecs()) {
      out.timeout(Duration.ofMinutes(o.getTimeoutMsecs()));
    }
    if (o.hasParentSpanId()) {
      out.parentSpan(spans.get(o.getParentSpanId()));
    }

    return out;
  }

  private @Nullable
  static AllowQueryingSearchIndexOptions createOptions(AllowQuerying request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (!request.hasOptions()) {
      return null;
    }

    var out = AllowQueryingSearchIndexOptions.allowQueryingSearchIndexOptions();
    var o = request.getOptions();

    if (o.hasTimeoutMsecs()) {
      out.timeout(Duration.ofMinutes(o.getTimeoutMsecs()));
    }
    if (o.hasParentSpanId()) {
      out.parentSpan(spans.get(o.getParentSpanId()));
    }

    return out;
  }

  private @Nullable
  static DisallowQueryingSearchIndexOptions createOptions(DisallowQuerying request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (!request.hasOptions()) {
      return null;
    }

    var out = DisallowQueryingSearchIndexOptions.disallowQueryingSearchIndexOptions();
    var o = request.getOptions();

    if (o.hasTimeoutMsecs()) {
      out.timeout(Duration.ofMinutes(o.getTimeoutMsecs()));
    }
    if (o.hasParentSpanId()) {
      out.parentSpan(spans.get(o.getParentSpanId()));
    }

    return out;
  }

  private @Nullable
  static FreezePlanSearchIndexOptions createOptions(FreezePlan request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (!request.hasOptions()) {
      return null;
    }

    var out = FreezePlanSearchIndexOptions.freezePlanSearchIndexOptions();
    var o = request.getOptions();

    if (o.hasTimeoutMsecs()) {
      out.timeout(Duration.ofMinutes(o.getTimeoutMsecs()));
    }
    if (o.hasParentSpanId()) {
      out.parentSpan(spans.get(o.getParentSpanId()));
    }

    return out;
  }

  private @Nullable
  static UnfreezePlanSearchIndexOptions createOptions(UnfreezePlan request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (!request.hasOptions()) {
      return null;
    }

    var out = UnfreezePlanSearchIndexOptions.unfreezePlanSearchIndexOptions();
    var o = request.getOptions();

    if (o.hasTimeoutMsecs()) {
      out.timeout(Duration.ofMinutes(o.getTimeoutMsecs()));
    }
    if (o.hasParentSpanId()) {
      out.parentSpan(spans.get(o.getParentSpanId()));
    }

    return out;
  }

  private @Nullable
  static AnalyzeDocumentOptions createOptions(AnalyzeDocument request, ConcurrentHashMap<String, RequestSpan> spans) {
    if (!request.hasOptions()) {
      return null;
    }

    var out = AnalyzeDocumentOptions.analyzeDocumentOptions();
    var o = request.getOptions();

    if (o.hasTimeoutMsecs()) {
      out.timeout(Duration.ofMinutes(o.getTimeoutMsecs()));
    }
    if (o.hasParentSpanId()) {
      out.parentSpan(spans.get(o.getParentSpanId()));
    }

    return out;
  }

  // [if:3.6.0]
  private static VectorQuery toSdk(com.couchbase.client.protocol.sdk.search.VectorQuery fit) {

    // [if:3.7.0]
    if (fit.hasBase64VectorQuery()) {
      VectorQuery sdk = VectorQuery.create(fit.getVectorFieldName(), fit.getBase64VectorQuery());
      applyVectorQueryOptions(sdk, fit);
      return sdk;
    }
    // [end]

    var floats = Floats.toArray(fit.getVectorQueryList());
    VectorQuery sdk = VectorQuery.create(fit.getVectorFieldName(), floats);
    applyVectorQueryOptions(sdk, fit);
    return sdk;
  }
  // [end]

  // [if:3.6.0]
  private static void applyVectorQueryOptions(VectorQuery sdk, com.couchbase.client.protocol.sdk.search.VectorQuery fit) {
    if (fit.hasOptions()) {
      var opts = fit.getOptions();
      if (opts.hasNumCandidates()) {
        sdk.numCandidates(opts.getNumCandidates());
      }
      if (opts.hasBoost()) {
        sdk.boost(opts.getBoost());
      }
    }
  }
  // [end]

  // [if:3.6.0]
  private static SearchRequest convertSearchRequest(com.couchbase.client.protocol.sdk.search.SearchRequest sr) {
    if (sr.hasSearchQuery()) {
      var out = SearchRequest.create(convertSearchQuery(sr.getSearchQuery()));
      if (sr.hasVectorSearch()) {
        out.vectorSearch(convertVectorSearch(sr.getVectorSearch()));
      }
      return out;
    }
    else if (sr.hasVectorSearch()) {
      return SearchRequest.create(convertVectorSearch(sr.getVectorSearch()));
    }
    return SearchRequest.create((SearchQuery) null);
  }

  private static VectorSearch convertVectorSearch(com.couchbase.client.protocol.sdk.search.VectorSearch vs) {
    var vectors = vs.getVectorQueryList().stream()
            .map(SearchHelper::toSdk)
            .toList();
    if (vs.hasOptions()) {
      return VectorSearch.create(vectors, convertVectorSearchOptions(vs.getOptions()));
    }
    return VectorSearch.create(vectors);
  }

  private static VectorSearchOptions convertVectorSearchOptions(com.couchbase.client.protocol.sdk.search.VectorSearchOptions options) {
    var out = VectorSearchOptions.vectorSearchOptions();
    if (options.hasVectorQueryCombination()) {
      out = out.vectorQueryCombination(switch (options.getVectorQueryCombination()) {
        case AND -> VectorQueryCombination.AND;
        case OR -> VectorQueryCombination.OR;
        default -> throw new UnsupportedOperationException("Unknown VectorQueryCombination");
      });
    }
    return out;
  }
  // [end]
}

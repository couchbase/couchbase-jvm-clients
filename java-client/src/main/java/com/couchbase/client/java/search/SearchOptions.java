/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.java.search;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreHighlightStyle;
import com.couchbase.client.core.api.search.CoreSearchOptions;
import com.couchbase.client.core.api.search.CoreSearchScanConsistency;
import com.couchbase.client.core.api.search.facet.CoreSearchFacet;
import com.couchbase.client.core.api.search.sort.CoreSearchSort;
import com.couchbase.client.core.api.shared.CoreMutationState;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.util.CbCollections;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryOptionsUtil;
import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.search.sort.SearchSort;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class SearchOptions extends CommonOptions<SearchOptions> {
  private @Nullable List<String> collections;
  private SearchScanConsistency consistency;
  private MutationState consistentWith;
  private boolean disableScoring = false;
  private Boolean explain;
  private Map<String, SearchFacet> facets;
  private @Nullable List<String> fields;
  private @Nullable List<String> highlightFields;
  private HighlightStyle highlightStyle;
  private Integer limit;
  private Map<String, Object> raw;
  private JsonSerializer serializer;
  private Integer skip;
  private @Nullable List<CoreSearchSort> sort;
  private @Nullable List<String> sortString;
  private Boolean includeLocations;

  public static SearchOptions searchOptions() {
    return new SearchOptions();
  }

  private SearchOptions() {
  }

  /**
   * Allows providing custom JSON key/value pairs for advanced usage.
   * <p>
   * If available, it is recommended to use the methods on this object to customize the search query. This method should
   * only be used if no such setter can be found (i.e. if an undocumented property should be set or you are using
   * an older client and a new server-configuration property has been added to the cluster).
   * <p>
   * Note that the value will be passed through a JSON encoder, so do not provide already encoded JSON as the value. If
   * you want to pass objects or arrays, you can use {@link JsonObject} and {@link JsonArray} respectively.
   *
   * @param key   the parameter name (key of the JSON property)  or empty.
   * @param value the parameter value (value of the JSON property).
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public SearchOptions raw(final String key, final Object value) {
    notNullOrEmpty(key, "Key");
    if (raw == null) {
      raw = new HashMap<>();
    }
    raw.put(key, value);
    return this;
  }

  /**
   * Add a limit to the query on the number of rows it can return.
   *
   * @param limit the maximum number of rows to return.
   * @return this SearchQuery for chaining.
   */
  public SearchOptions limit(int limit) {
    this.limit = limit;
    return this;
  }

  /**
   * Set the number of rows to skip (eg. for pagination).
   *
   * @param skip the number of results to skip.
   * @return this SearchQuery for chaining.
   */
  public SearchOptions skip(int skip) {
    this.skip = skip;
    return this;
  }

  /**
   * Activates or deactivates the explanation of each result hit in the response, according to the parameter.
   *
   * @param explain should the response include an explanation of each hit (true) or not (false)?
   * @return this SearchQuery for chaining.
   */
  public SearchOptions explain(boolean explain) {
    this.explain = explain;
    return this;
  }

  /**
   * Configures the highlighting of matches in the response.
   * <p>
   * This drives the inclusion of the {@link SearchRow#fragments() fragments} in each {@link SearchRow hit}.
   * <p>
   * Note that to be highlighted, the fields must be stored in the FTS index.
   *
   * @param style  the {@link HighlightStyle} to apply.
   * @param fields the optional fields on which to highlight. If none, all fields where there is a match are highlighted.
   * @return this SearchQuery for chaining.
   */
  public SearchOptions highlight(HighlightStyle style, String... fields) {
    this.highlightStyle = style;
    this.highlightFields = new ArrayList<>(Arrays.asList(requireNonNull(fields)));
    return this;
  }

  /**
   * Configures the highlighting of matches in the response, for the specified fields and using the server's default
   * highlighting style.
   * <p>
   * This drives the inclusion of the {@link SearchRow#fragments() fragments} in each {@link SearchRow hit}.
   * <p>
   * Note that to be highlighted, the fields must be stored in the FTS index.
   *
   * @param fields the optional fields on which to highlight. If none, all fields where there is a match are highlighted.
   * @return this SearchQuery for chaining.
   */
  public SearchOptions highlight(String... fields) {
    return highlight(HighlightStyle.SERVER_DEFAULT, fields);
  }

  /**
   * Configures the highlighting of matches in the response for all fields, using the server's default highlighting
   * style.
   * <p>
   * This drives the inclusion of the {@link SearchRow#fragments() fragments} in each {@link SearchRow hit}.
   * <p>
   * Note that to be highlighted, the fields must be stored in the FTS index.
   *
   * @return this SearchQuery for chaining.
   */
  public SearchOptions highlight() {
    return highlight(HighlightStyle.SERVER_DEFAULT);
  }

  /**
   * Configures the list of fields for which the whole value should be included in the response. If empty, no field
   * values are included.
   * <p>
   * This drives the inclusion of the fields in each {@link SearchRow hit}.
   * <p>
   * Note that to be highlighted, the fields must be stored in the FTS index.
   *
   * @param fields the fields to include.
   * @return this SearchQuery for chaining.
   */
  public SearchOptions fields(String... fields) {
    this.fields = new ArrayList<>(Arrays.asList(requireNonNull(fields)));
    return this;
  }

  /**
   * Allows to limit the search query to a specific list of collection names.
   * <p>
   * NOTE: this is only supported with server 7.0 and later.
   *
   * @param collectionNames the names of the collections this query should be limited to.
   * @return this SearchQuery for chaining.
   */
  public SearchOptions collections(String... collectionNames) {
    this.collections = new ArrayList<>(Arrays.asList(requireNonNull(collectionNames)));
    return this;
  }

  /**
   * Sets the unparameterized consistency to consider for this FTS query. This replaces any
   * consistency tuning previously set.
   *
   * @param consistency the simple consistency to use.
   * @return this SearchQuery for chaining.
   */
  public SearchOptions scanConsistency(SearchScanConsistency consistency) {
    this.consistency = consistency;
    consistentWith = null;
    return this;
  }

  /**
   * Sets mutation tokens this query should be consistent with.
   *
   * @param consistentWith the mutation state to be consistent with.
   * @return this {@link QueryOptions} for chaining.
   */
  public SearchOptions consistentWith(final MutationState consistentWith) {
    this.consistentWith = consistentWith;
    consistency = null;
    return this;
  }

  /**
   * Configures the list of fields (including special fields) which are used for sorting purposes. If empty, the
   * default sorting (descending by score) is used by the server.
   * <p>
   * The list of sort fields can include actual fields (like "firstname" but then they must be stored in the index,
   * configured in the server side mapping). Fields provided first are considered first and in a "tie" case the
   * next sort field is considered. So sorting by "firstname" and then "lastname" will first sort ascending by
   * the firstname and if the names are equal then sort ascending by lastname. Special fields like "_id" and "_score"
   * can also be used. If prefixed with "-" the sort order is set to descending.
   * <p>
   * If no sort is provided, it is equal to sort("-_score"), since the server will sort it by score in descending
   * order.
   *
   * @param sort the fields that should take part in the sorting.
   * @return this SearchQuery for chaining.
   */
  public SearchOptions sort(Object... sort) {
    if (sort != null) {
      for (Object o : sort) {
        if (o instanceof String) {
          if (this.sortString == null) {
            this.sortString = new ArrayList<>();
          }
          this.sortString.add((String) o);
        } else if (o instanceof SearchSort) {
          if (this.sort == null) {
            this.sort = new ArrayList<>();
          }
          this.sort.add(((SearchSort) o).toCore());
        } else {
          throw InvalidArgumentException.fromMessage("Only String or SearchSort " +
            "instances are allowed as sort arguments!");
        }
      }
    }
    return this;
  }

  /**
   * Adds one {@link SearchFacet} to the query.
   * <p>
   * This is an additive operation (the given facets are added to any facet previously requested),
   * but if an existing facet has the same name it will be replaced.
   * <p>
   * This drives the inclusion of the facets in the {@link SearchResult}.
   * <p>
   * Note that to be faceted, a field's value must be stored in the FTS index.
   *
   * @param facets the facets to add to the query.
   */
  public SearchOptions facets(final Map<String, SearchFacet> facets) {
    this.facets = facets;
    return this;
  }

  public SearchOptions serializer(final JsonSerializer serializer) {
    this.serializer = serializer;
    return this;
  }

  /**
   * If set to true, thee server will not perform any scoring on the hits.
   *
   * @param disableScoring if scoring should be disabled.
   * @return these {@link SearchOptions} for chaining purposes.
   */
  public SearchOptions disableScoring(final boolean disableScoring) {
    this.disableScoring = disableScoring;
    return this;
  }

  /**
   * If set to true, will include the {@link SearchRow#locations()}.
   *
   * @param includeLocations set to true to include the locations.
   * @return these {@link SearchOptions} for chaining purposes.
   */
  public SearchOptions includeLocations(final boolean includeLocations) {
    this.includeLocations = includeLocations;
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions implements CoreSearchOptions {

    Built() {
    }

    public JsonSerializer serializer() {
      return serializer;
    }

    @Override
    public List<String> collections() {
      return collections == null ? Collections.emptyList() : collections;
    }

    @Override
    public CoreSearchScanConsistency consistency() {
      if (consistency == SearchScanConsistency.NOT_BOUNDED) {
        return CoreSearchScanConsistency.NOT_BOUNDED;
      }
      return null;
    }

    @Override
    public CoreMutationState consistentWith() {
      if (consistentWith == null) {
        return null;
      }
      return new CoreMutationState(consistentWith);
    }

    @Override
    public Boolean disableScoring() {
      return disableScoring;
    }

    @Override
    public Boolean explain() {
      return explain;
    }

    @Override
    public Map<String, CoreSearchFacet> facets() {
      if (facets == null) {
        return Collections.emptyMap();
      }

      Map<String, CoreSearchFacet> out = new HashMap<>();
      facets.forEach((k, v) -> out.put(k, v.toCore()));
      return out;
    }

    @Override
    public List<String> fields() {
      return fields == null ? Collections.emptyList() : fields;
    }

    @Override
    public List<String> highlightFields() {
      return highlightFields == null ? Collections.emptyList() : highlightFields;
    }

    @Override
    public CoreHighlightStyle highlightStyle() {
      if (highlightStyle == null) {
        return null;
      }
      switch (highlightStyle) {
        case HTML:
          return CoreHighlightStyle.HTML;
        case ANSI:
          return CoreHighlightStyle.ANSI;
        case SERVER_DEFAULT:
          return CoreHighlightStyle.SERVER_DEFAULT;
        default:
          throw new IllegalStateException("Internal bug - unknown highlight style");
      }
    }

    @Override
    public Integer limit() {
      return limit;
    }

    @Override
    public JsonNode raw() {
      if (raw == null) {
        return null;
      }

      return QueryOptionsUtil.convert(raw);
    }

    @Override
    public Integer skip() {
      return skip;
    }

    @Override
    public List<CoreSearchSort> sort() {
      return sort == null ? Collections.emptyList() : sort;
    }

    @Override
    public List<String> sortString() {
      return sortString == null ? Collections.emptyList() : sortString;
    }

    @Override
    public Boolean includeLocations() {
      return includeLocations;
    }

    @Override
    public CoreCommonOptions commonOptions() {
      return this;
    }
  }

}

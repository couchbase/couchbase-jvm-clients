/*
 * Copyright (c) 2016 Couchbase, Inc.
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

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.search.queries.BooleanFieldQuery;
import com.couchbase.client.java.search.queries.BooleanQuery;
import com.couchbase.client.java.search.queries.ConjunctionQuery;
import com.couchbase.client.java.search.queries.DateRangeQuery;
import com.couchbase.client.java.search.queries.DisjunctionQuery;
import com.couchbase.client.java.search.queries.DocIdQuery;
import com.couchbase.client.java.search.queries.GeoBoundingBoxQuery;
import com.couchbase.client.java.search.queries.GeoDistanceQuery;
import com.couchbase.client.java.search.queries.GeoPolygonQuery;
import com.couchbase.client.java.search.queries.MatchAllQuery;
import com.couchbase.client.java.search.queries.MatchNoneQuery;
import com.couchbase.client.java.search.queries.MatchPhraseQuery;
import com.couchbase.client.java.search.queries.MatchQuery;
import com.couchbase.client.java.search.queries.NumericRangeQuery;
import com.couchbase.client.java.search.queries.PhraseQuery;
import com.couchbase.client.java.search.queries.PrefixQuery;
import com.couchbase.client.java.search.queries.QueryStringQuery;
import com.couchbase.client.java.search.queries.RegexpQuery;
import com.couchbase.client.java.search.queries.TermQuery;
import com.couchbase.client.java.search.queries.TermRangeQuery;
import com.couchbase.client.java.search.queries.WildcardQuery;
import com.couchbase.client.java.util.Coordinate;

import java.util.List;

/**
 * A base class for all FTS query classes. Exposes the common FTS query parameters.
 * In order to instantiate various flavors of queries, look at concrete classes or
 * static factory methods in {@link SearchQuery}.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
@Stability.Internal
public abstract class SearchQuery {

    private Double boost;

    protected SearchQuery() { }

    public SearchQuery boost(double boost) {
        this.boost = boost;
        return this;
    }


    public void injectParamsAndBoost(JsonObject input) {
        if (boost != null) {
            input.put("boost", boost);
        }
        injectParams(input);
    }


    protected abstract void injectParams(JsonObject input);

    /**
     * Exports the whole query as a {@link JsonObject}.
     *
     * @see #injectParams(JsonObject) for the part that deals with global parameters
     */
    public JsonObject export() {
        JsonObject result = JsonObject.create();
        injectParams(result);

        JsonObject queryJson = JsonObject.create();
        injectParamsAndBoost(queryJson);
        return result.put("query", queryJson);
    }

    /**
     * @return the String representation of the FTS query, which is its JSON representation without global parameters.
     */
    @Override
    public String toString() {
        JsonObject json = JsonObject.create();
        injectParamsAndBoost(json);
        return json.toString();
    }

    /** Prepare a {@link QueryStringQuery} body. */
    public static QueryStringQuery queryString(String query) {
        return new QueryStringQuery(query);
    }

    /** Prepare a {@link MatchQuery} body. */
    public static MatchQuery match(String match) {
        return new MatchQuery(match);
    }

    /** Prepare a {@link MatchPhraseQuery} body. */
    public static MatchPhraseQuery matchPhrase(String matchPhrase) {
        return new MatchPhraseQuery(matchPhrase);
    }

    /** Prepare a {@link PrefixQuery} body. */
    public static PrefixQuery prefix(String prefix) {
        return new PrefixQuery(prefix);
    }

    /** Prepare a {@link RegexpQuery} body. */
    public static RegexpQuery regexp(String regexp) {
        return new RegexpQuery(regexp);
    }

    /** Prepare a {@link TermRangeQuery} body. */
    public static TermRangeQuery termRange() {
        return new TermRangeQuery();
    }

    /** Prepare a {@link NumericRangeQuery} body. */
    public static NumericRangeQuery numericRange() {
        return new NumericRangeQuery();
    }

    /** Prepare a {@link DateRangeQuery} body. */
    public static DateRangeQuery dateRange() {
        return new DateRangeQuery();
    }

    /** Prepare a {@link DisjunctionQuery} body. */
    public static DisjunctionQuery disjuncts(SearchQuery... queries) {
        return new DisjunctionQuery(queries);
    }

    /** Prepare a {@link ConjunctionQuery} body. */
    public static ConjunctionQuery conjuncts(SearchQuery... queries) {
        return new ConjunctionQuery(queries);
    }

    /** Prepare a {@link BooleanQuery} body. */
    public static BooleanQuery booleans() {
        return new BooleanQuery();
    }

    /** Prepare a {@link WildcardQuery} body. */
    public static WildcardQuery wildcard(String wildcard) {
        return new WildcardQuery(wildcard);
    }

    /** Prepare a {@link DocIdQuery} body. */
    public static DocIdQuery docId(String... docIds) {
        return new DocIdQuery(docIds);
    }

    /** Prepare a {@link BooleanFieldQuery} body. */
    public static BooleanFieldQuery booleanField(boolean value) {
        return new BooleanFieldQuery(value);
    }

    /** Prepare a {@link TermQuery} body. */
    public static TermQuery term(String term) {
        return new TermQuery(term);
    }

    /** Prepare a {@link PhraseQuery} body. */
    public static PhraseQuery phrase(String... terms) {
        return new PhraseQuery(terms);
    }

    /** Prepare a {@link MatchAllQuery} body. */
    public static MatchAllQuery matchAll() {
        return new MatchAllQuery();
    }

    /** Prepare a {@link MatchNoneQuery} body. */
    public static MatchNoneQuery matchNone() {
        return new MatchNoneQuery();
    }

    /** Prepare a {@link GeoBoundingBoxQuery} body. */
    public static GeoBoundingBoxQuery geoBoundingBox(double topLeftLon, double topLeftLat,
                                                     double bottomRightLon, double bottomRightLat) {
        return new GeoBoundingBoxQuery(topLeftLon, topLeftLat, bottomRightLon, bottomRightLat);
    }

    /** Prepare a {@link GeoBoundingBoxQuery} body. */
    @Stability.Uncommitted
    public static GeoBoundingBoxQuery geoBoundingBox(final Coordinate topLeftCoordinate,
                                                     final Coordinate bottomRightCoordinate) {
        return geoBoundingBox(topLeftCoordinate.lon(), topLeftCoordinate.lat(), bottomRightCoordinate.lon(),
          bottomRightCoordinate.lat());
    }

    /** Prepare a {@link GeoDistanceQuery} body. */
    public static GeoDistanceQuery geoDistance(double locationLon, double locationLat, String distance) {
        return new GeoDistanceQuery(locationLon, locationLat, distance);
    }

    /** Prepare a {@link GeoDistanceQuery} body. */
    @Stability.Uncommitted
    public static GeoDistanceQuery geoDistance(final Coordinate locationCoordinate, final String distance) {
        return geoDistance(locationCoordinate.lon(), locationCoordinate.lat(), distance);
    }

    /** Prepare a {@link GeoPolygonQuery} body. */
    @Stability.Uncommitted
    @SinceCouchbase("6.5.1")
    public static GeoPolygonQuery geoPolygon(final List<Coordinate> coordinates) {
        return new GeoPolygonQuery(coordinates);
    }

}

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
package com.couchbase.client.java.search.result;

import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.msg.search.SearchChunkRow;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.search.HighlightStyle;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.rows.DefaultRowLocations;
import com.couchbase.client.java.search.result.rows.RowLocations;

import java.io.IOException;
import java.util.*;

/**
 * An FTS result row (or hit).
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
public class SearchQueryRow {
    private final String index;
    private final String id;
    private final double score;
    private final JsonObject explanation;
    private final RowLocations locations;
    private final Map<String, List<String>> fragments;
    private final Map<String, String> fields;

    public SearchQueryRow(String index, String id, double score, JsonObject explanation, RowLocations locations,
                          Map<String, List<String>> fragments, Map<String, String> fields) {
        this.index = index;
        this.id = id;
        this.score = score;
        this.explanation = explanation;
        this.locations = locations;
        this.fragments = fragments;
        this.fields = fields;
    }

    /**
     * The name of the FTS pindex that gave this result.
     */
    public String index() {
        return this.index;
    }

    /**
     * The id of the matching document.
     */
    public String id() {
        return this.id;
    }

    /**
     * The score of this hit.
     */
    public double score() {
        return this.score;
    }

    /**
     * If {@link SearchQuery#explain() requested in the query}, an explanation of the match, in JSON form.
     */
    public JsonObject explanation() {
        return this.explanation;
    }

    /**
     * This rows's location, as an {@link RowLocations} map-like object.
     */
    public RowLocations locations() {
        return this.locations;
    }

    /**
     * The fragments for each field that was requested as highlighted
     * (as defined in the {@link SearchQuery#highlight(HighlightStyle, String...) SearchParams}).
     * <p>
     * A fragment is an extract of the field's value where the matching terms occur.
     * Matching terms are surrounded by a <code>&lt;match&gt;</code> tag.
     *
     * @return the fragments as a {@link Map}. Keys are the fields.
     */
    public Map<String, List<String>> fragments() {
        return this.fragments;
    }

    /**
     * The value of each requested field (as defined in the {@link SearchQuery}.
     *
     * @return the fields values as a {@link Map}. Keys are the fields.
     */
    public Map<String, String> fields() {
        return this.fields;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SearchQueryRow that = (SearchQueryRow) o;

        if (Double.compare(that.score, score) != 0) {
            return false;
        }
        if (!index.equals(that.index)) {
            return false;
        }
        return id.equals(that.id);

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = index.hashCode();
        result = 31 * result + id.hashCode();
        temp = Double.doubleToLongBits(score);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public static SearchQueryRow fromResponse(SearchChunkRow row) {
        try {
            JsonObject hit = JacksonTransformers.MAPPER.readValue(row.data(), JsonObject.class);

            String index = hit.getString("index");
            String id = hit.getString("id");
            double score = hit.getDouble("score");
            JsonObject explanationJson = hit.getObject("explanation");
            if (explanationJson == null) {
                explanationJson = JsonObject.empty();
            }

            RowLocations locations = DefaultRowLocations.from(hit.getObject("locations"));

            JsonObject fragmentsJson = hit.getObject("fragments");
            Map<String, List<String>> fragments;
            if (fragmentsJson != null) {
                fragments = new HashMap<String, List<String>>(fragmentsJson.size());
                for (String field : fragmentsJson.getNames()) {
                    List<String> fragment;
                    JsonArray fragmentJson = fragmentsJson.getArray(field);
                    if (fragmentJson != null) {
                        fragment = new ArrayList<String>(fragmentJson.size());
                        for (int i = 0; i < fragmentJson.size(); i++) {
                            fragment.add(fragmentJson.getString(i));
                        }
                    } else {
                        fragment = Collections.emptyList();
                    }
                    fragments.put(field, fragment);
                }
            } else {
                fragments = Collections.emptyMap();
            }

            Map<String, String> fields;
            JsonObject fieldsJson = hit.getObject("fields");
            if (fieldsJson != null) {
                fields = new HashMap<String, String>(fieldsJson.size());
                for (String f : fieldsJson.getNames()) {
                    fields.put(f, String.valueOf(fieldsJson.get(f)));
                }
            } else {
                fields = Collections.emptyMap();
            }

            return new SearchQueryRow(index, id, score, explanationJson, locations, fragments, fields);
        } catch (IOException e) {
            throw new DecodingFailedException("Failed to decode row '" + new String(row.data()) + "'", e);
        }

    }

    @Override
    public String toString() {
        return "DefaultSearchQueryRow{" +
                "index='" + index + '\'' +
                ", id='" + id + '\'' +
                ", score=" + score +
                ", explanation=" + explanation +
                ", locations=" + locations +
                ", fragments=" + fragments +
                ", fields=" + fields +
                '}';
    }
}

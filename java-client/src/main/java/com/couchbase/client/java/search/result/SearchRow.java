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

import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.msg.search.SearchChunkRow;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.search.HighlightStyle;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static com.couchbase.client.core.util.CbObjects.defaultIfNull;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * An FTS result row (or hit).
 *
 * @since 2.3.0
 */
public class SearchRow {
    private final String index;
    private final String id;
    private final double score;
    private final JsonObject explanation;
    private final Optional<SearchRowLocations> locations;
    private final Map<String, List<String>> fragments;
    private final byte[] fields;
    private final JsonSerializer serializer;

    public SearchRow(String index, String id, double score, JsonObject explanation, Optional<SearchRowLocations> locations,
                     Map<String, List<String>> fragments, byte[] fields, JsonSerializer serializer) {
        this.index = index;
        this.id = id;
        this.score = score;
        this.explanation = explanation;
        this.locations = locations;
        this.fragments = fragments;
        this.fields = fields;
        this.serializer = serializer;
    }

    /**
     * The name of the FTS index that gave this result.
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
     * If {@link SearchOptions#explain(boolean)} () requested in the query}, an explanation of the match, in JSON form.
     */
    public JsonObject explanation() {
        return this.explanation;
    }

    /**
     * This rows's location, as an {@link SearchRowLocations} map-like object.
     */
    public Optional<SearchRowLocations> locations() {
        return this.locations;
    }

    /**
     * The fragments for each field that was requested as highlighted
     * (as defined in the {@link SearchOptions#highlight(HighlightStyle, String...) SearchParams}).
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
     * @return the fields mapped to the given target type
     */
    public <T> T fieldsAs(final Class<T> target) {
        return serializer.deserialize(target, fields);
    }

    /**
     * The value of each requested field (as defined in the {@link SearchQuery}.
     *
     * @return the fields mapped to the given target type
     */
    public <T> T fieldsAs(final TypeRef<T> target) {
        return serializer.deserialize(target, fields);
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SearchRow that = (SearchRow) o;

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

    public static SearchRow fromResponse(final SearchChunkRow row, final JsonSerializer serializer) {
        try {
            JsonObject hit = JacksonTransformers.MAPPER.readValue(row.data(), JsonObject.class);

            String index = hit.getString("index");
            String id = hit.getString("id");
            double score = hit.getDouble("score");
            JsonObject explanationJson = defaultIfNull(hit.getObject("explanation"), JsonObject::create);

            Optional<SearchRowLocations> locations = Optional.ofNullable(hit.getObject("locations"))
                .map(SearchRowLocations::from);

            JsonObject fragmentsJson = hit.getObject("fragments");
            Map<String, List<String>> fragments;
            if (fragmentsJson != null) {
                fragments = new HashMap<>(fragmentsJson.size());
                for (String field : fragmentsJson.getNames()) {
                    List<String> fragment;
                    JsonArray fragmentJson = fragmentsJson.getArray(field);
                    if (fragmentJson != null) {
                        fragment = new ArrayList<>(fragmentJson.size());
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

            byte[] fields = null;
            if (hit.containsKey("fields")) {
                // daschl: this is a bit wasteful and should be streamlined
                JacksonTransformers.MAPPER.writeValueAsBytes(hit.getObject("fields").toMap());
            }
            return new SearchRow(index, id, score, explanationJson, locations, fragments, fields, serializer);
        } catch (IOException e) {
            throw new DecodingFailureException("Failed to decode row '" + new String(row.data(), UTF_8) + "'", e);
        }

    }

    @Override
    public String toString() {
        return "SearchRow{" +
            "index='" + redactMeta(index) + '\'' +
            ", id='" + id + '\'' +
            ", score=" + score +
            ", explanation=" + explanation +
            ", locations=" + redactUser(locations) +
            ", fragments=" + redactUser(fragments) +
            ", fields=" + redactUser(fields) +
            '}';
    }
}

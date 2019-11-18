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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.util.*;

/**
 * A default implementation of a {@link SearchRowLocations}.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
@Stability.Volatile
public class SearchRowLocations {

    private final Map<String, Map<String, List<SearchRowLocation>>> locations = new HashMap<String, Map<String, List<SearchRowLocation>>>();
    private int size;

    private SearchRowLocations add(SearchRowLocation l) {
        //note: it is not expected that multiple threads concurrently add a RowLocation, as even a
        //streaming parser would parse a single hit in its entirety, not individual locations.
        Map<String, List<SearchRowLocation>> byTerm = locations.computeIfAbsent(l.field(), k -> new HashMap<>());

        List<SearchRowLocation> list = byTerm.computeIfAbsent(l.term(), k -> new ArrayList<>());

        list.add(l);
        size++;
        return this;
    }

    public List<SearchRowLocation> get(String field) {
        Map<String, List<SearchRowLocation>> byTerm = locations.get(field);
        if (byTerm == null) {
            return Collections.emptyList();
        }

        List<SearchRowLocation> result = new LinkedList<>();
        for (List<SearchRowLocation> termList : byTerm.values()) {
            result.addAll(termList);
        }
        return result;
    }

    public List<SearchRowLocation> get(String field, String term) {
        Map<String, List<SearchRowLocation>> byTerm = locations.get(field);
        if (byTerm == null) {
            return Collections.emptyList();
        }

        List<SearchRowLocation> result = byTerm.get(term);
        if (result == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(result);
    }

    public List<SearchRowLocation> getAll() {
        List<SearchRowLocation> all = new LinkedList<>();
        for (Map.Entry<String, Map<String, List<SearchRowLocation>>> terms : locations.entrySet()) {
            for (List<SearchRowLocation> rowLocations : terms.getValue().values()) {
                all.addAll(rowLocations);
            }
        }
        return all;
    }

    public List<String> fields() {
        return new ArrayList(locations.keySet());
    }

    public List<String> termsFor(String field) {
        final Map<String, List<SearchRowLocation>> termMap = locations.get(field);
        if (termMap == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(termMap.keySet());
    }

    public Set<String> terms() {
        Set<String> termSet = new HashSet<>();
        for (Map<String,List<SearchRowLocation>> termMap : locations.values()) {
            termSet.addAll(termMap.keySet());
        }
        return termSet;
    }

    /**
     * Parses a FTS JSON representation of a {@link SearchRowLocations}.
     */
    public static SearchRowLocations from(JsonObject locationsJson) {
        SearchRowLocations hitLocations = new SearchRowLocations();
        if (locationsJson == null) {
            return hitLocations;
        }

        for (String field : locationsJson.getNames()) {
            JsonObject termsJson = locationsJson.getObject(field);

            for (String term : termsJson.getNames()) {
                JsonArray locsJson = termsJson.getArray(term);

                for (int i = 0; i < locsJson.size(); i++) {
                    JsonObject loc = locsJson.getObject(i);
                    long pos = loc.getLong("pos");
                    long start = loc.getLong("start");
                    long end = loc.getLong("end");
                    JsonArray arrayPositionsJson = loc.getArray("array_positions");
                    long[] arrayPositions = null;
                    if (arrayPositionsJson != null) {
                        arrayPositions = new long[arrayPositionsJson.size()];
                        for (int j = 0; j < arrayPositionsJson.size(); j++) {
                            arrayPositions[j] = arrayPositionsJson.getLong(j);
                        }
                    }
                    hitLocations.add(new SearchRowLocation(field, term, pos, start, end, arrayPositions));
                }
            }
        }
        return hitLocations;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SearchRowLocations{")
                .append("size=").append(size)
                .append(", locations=[");

        for (Map<String, List<SearchRowLocation>> map : locations.values()) {
            for (List<SearchRowLocation> rowLocations : map.values()) {
                for (SearchRowLocation rowLocation : rowLocations) {
                    sb.append(rowLocation).append(",");
                }
            }
        }

        if (!locations.isEmpty()) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("]}");
        return sb.toString();
    }
}

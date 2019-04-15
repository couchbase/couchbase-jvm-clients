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
package com.couchbase.client.java.search.result.rows;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.util.*;

/**
 * A default implementation of a {@link RowLocations}.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
@Stability.Volatile
public class DefaultRowLocations implements RowLocations {

    private final Map<String, Map<String, List<RowLocation>>> locations = new HashMap<String, Map<String, List<RowLocation>>>();
    private int size;

    @Override
    public RowLocations add(RowLocation l) {
        //note: it is not expected that multiple threads concurrently add a RowLocation, as even a
        //streaming parser would parse a single hit in its entirety, not individual locations.
        Map<String, List<RowLocation>> byTerm = locations.get(l.field());
        if (byTerm == null) {
            byTerm = new HashMap<String, List<RowLocation>>();
            locations.put(l.field(), byTerm);
        }

        List<RowLocation> list = byTerm.get(l.term());
        if (list == null) {
            list = new ArrayList<RowLocation>();
            byTerm.put(l.term(), list);
        }

        list.add(l);
        size++;
        return this;
    }

    @Override
    public List<RowLocation> get(String field) {
        Map<String, List<RowLocation>> byTerm = locations.get(field);
        if (byTerm == null) {
            return Collections.emptyList();
        }

        List<RowLocation> result = new LinkedList<RowLocation>();
        for (List<RowLocation> termList : byTerm.values()) {
            result.addAll(termList);
        }
        return result;
    }

    @Override
    public List<RowLocation> get(String field, String term) {
        Map<String, List<RowLocation>> byTerm = locations.get(field);
        if (byTerm == null) {
            return Collections.emptyList();
        }

        List<RowLocation> result = byTerm.get(term);
        if (result == null) {
            return Collections.emptyList();
        }
        return new ArrayList<RowLocation>(result);
    }

    @Override
    public List<RowLocation> getAll() {
        List<RowLocation> all = new LinkedList<RowLocation>();
        for (Map.Entry<String, Map<String, List<RowLocation>>> terms : locations.entrySet()) {
            for (List<RowLocation> rowLocations : terms.getValue().values()) {
                all.addAll(rowLocations);
            }
        }
        return all;
    }

    @Override
    public long count() {
        return size;
    }

    @Override
    public List<String> fields() {
        return new ArrayList(locations.keySet());
    }

    @Override
    public List<String> termsFor(String field) {
        final Map<String, List<RowLocation>> termMap = locations.get(field);
        if (termMap == null) {
            return Collections.emptyList();
        }
        return new ArrayList<String>(termMap.keySet());
    }

    @Override
    public Set<String> terms() {
        Set<String> termSet = new HashSet<String>();
        for (Map<String,List<RowLocation>> termMap : locations.values()) {
            termSet.addAll(termMap.keySet());
        }
        return termSet;
    }

    /**
     * Parses a FTS JSON representation of a {@link RowLocations}.
     */
    public static RowLocations from(JsonObject locationsJson) {
        DefaultRowLocations hitLocations = new DefaultRowLocations();
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
                    hitLocations.add(new RowLocation(field, term, pos, start, end, arrayPositions));
                }
            }
        }
        return hitLocations;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DefaultRowLocations{")
                .append("size=").append(size)
                .append(", locations=[");

        for (Map<String, List<RowLocation>> map : locations.values()) {
            for (List<RowLocation> rowLocations : map.values()) {
                for (RowLocation rowLocation : rowLocations) {
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

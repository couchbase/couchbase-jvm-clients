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
package com.couchbase.client.core.api.search.result;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Stability.Internal
public class CoreSearchRowLocations {

    private final Map<String, Map<String, List<CoreSearchRowLocation>>> locations = new HashMap<>();
    private int size;

    private CoreSearchRowLocations add(CoreSearchRowLocation l) {
        //note: it is not expected that multiple threads concurrently add a RowLocation, as even a
        //streaming parser would parse a single hit in its entirety, not individual locations.
        Map<String, List<CoreSearchRowLocation>> byTerm = locations.computeIfAbsent(l.field(), k -> new HashMap<>());

        List<CoreSearchRowLocation> list = byTerm.computeIfAbsent(l.term(), k -> new ArrayList<>());

        list.add(l);
        size++;
        return this;
    }

    public List<CoreSearchRowLocation> get(String field) {
        Map<String, List<CoreSearchRowLocation>> byTerm = locations.get(field);
        if (byTerm == null) {
            return Collections.emptyList();
        }

        List<CoreSearchRowLocation> result = new LinkedList<>();
        for (List<CoreSearchRowLocation> termList : byTerm.values()) {
            result.addAll(termList);
        }
        return result;
    }

    public List<CoreSearchRowLocation> get(String field, String term) {
        Map<String, List<CoreSearchRowLocation>> byTerm = locations.get(field);
        if (byTerm == null) {
            return Collections.emptyList();
        }

        List<CoreSearchRowLocation> result = byTerm.get(term);
        if (result == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(result);
    }

    public List<CoreSearchRowLocation> getAll() {
        List<CoreSearchRowLocation> all = new LinkedList<>();
        for (Map.Entry<String, Map<String, List<CoreSearchRowLocation>>> terms : locations.entrySet()) {
            for (List<CoreSearchRowLocation> rowLocations : terms.getValue().values()) {
                all.addAll(rowLocations);
            }
        }
        return all;
    }

    public List<String> fields() {
        return new ArrayList(locations.keySet());
    }

    public List<String> termsFor(String field) {
        final Map<String, List<CoreSearchRowLocation>> termMap = locations.get(field);
        if (termMap == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(termMap.keySet());
    }

    public Set<String> terms() {
        Set<String> termSet = new HashSet<>();
        for (Map<String,List<CoreSearchRowLocation>> termMap : locations.values()) {
            termSet.addAll(termMap.keySet());
        }
        return termSet;
    }

   public static CoreSearchRowLocations from(List<CoreSearchRowLocation> locations) {
      CoreSearchRowLocations result = new CoreSearchRowLocations();
      locations.forEach(result::add);
      return result;
   }

    /**
     * Parses a FTS JSON representation of a {@link CoreSearchRowLocations}.
     */
    public static CoreSearchRowLocations from(ObjectNode locationsJson) {
        CoreSearchRowLocations hitLocations = new CoreSearchRowLocations();
        if (locationsJson == null) {
            return hitLocations;
        }

        locationsJson.fieldNames().forEachRemaining(field -> {
            ObjectNode termsJson = (ObjectNode) locationsJson.get(field);

            termsJson.fieldNames().forEachRemaining(term -> {
                ArrayNode locsJson = (ArrayNode) termsJson.get(term);

                for (int i = 0; i < locsJson.size(); i++) {
                    ObjectNode loc = (ObjectNode) locsJson.get(i);
                    long pos = loc.get("pos").longValue();
                    long start = loc.get("start").longValue();
                    long end = loc.get("end").asLong();
                    JsonNode positions = loc.get("array_positions");
                    long[] arrayPositions = null;
                    if (positions != null && positions.isArray()) {
                        ArrayNode arrayPositionsJson = (ArrayNode) positions;
                        arrayPositions = new long[arrayPositionsJson.size()];
                        for (int j = 0; j < arrayPositionsJson.size(); j++) {
                            arrayPositions[j] = arrayPositionsJson.get(j).longValue();
                        }
                    }
                    hitLocations.add(new CoreSearchRowLocation(field, term, pos, start, end, arrayPositions));
                }
            });
        });
        return hitLocations;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SearchRowLocations{")
                .append("size=").append(size)
                .append(", locations=[");

        for (Map<String, List<CoreSearchRowLocation>> map : locations.values()) {
            for (List<CoreSearchRowLocation> rowLocations : map.values()) {
                for (CoreSearchRowLocation rowLocation : rowLocations) {
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

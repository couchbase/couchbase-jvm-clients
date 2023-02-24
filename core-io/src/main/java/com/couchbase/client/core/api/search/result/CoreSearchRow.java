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

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.search.SearchChunkRow;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CoreSearchRow {
  private final String index;
  private final String id;
  private final double score;
  private final ObjectNode explanation;
  private final Optional<CoreSearchRowLocations> locations;
  private final Map<String, List<String>> fragments;
  private final byte[] fields;

  public CoreSearchRow(String index, String id, double score, ObjectNode explanation, Optional<CoreSearchRowLocations> locations,
                       Map<String, List<String>> fragments, byte[] fields) {
    this.index = index;
    this.id = id;
    this.score = score;
    this.explanation = explanation;
    this.locations = locations;
    this.fragments = fragments;
    this.fields = fields;
  }

  public String index() {
    return index;
  }

  public String id() {
    return id;
  }

  public double score() {
    return score;
  }

  public ObjectNode explanation() {
    return explanation == null ? Mapper.createObjectNode() : null;
  }

  public Optional<CoreSearchRowLocations> locations() {
    return locations;
  }

  public Map<String, List<String>> fragments() {
    return fragments;
  }

  @Nullable
  public byte[] fields() {
    return fields;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CoreSearchRow searchRow = (CoreSearchRow) o;
    return Double.compare(searchRow.score, score) == 0 &&
            Objects.equals(index, searchRow.index) &&
            Objects.equals(id, searchRow.id) &&
            Objects.equals(explanation, searchRow.explanation) &&
            Objects.equals(locations, searchRow.locations) &&
            Objects.equals(fragments, searchRow.fragments) &&
            Arrays.equals(fields, searchRow.fields);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(index, id, score, explanation, locations, fragments);
    result = 31 * result + Arrays.hashCode(fields);
    return result;
  }

  public static CoreSearchRow fromResponse(final SearchChunkRow row) {
    ObjectNode hit = (ObjectNode) Mapper.decodeIntoTree(row.data());
    return fromResponse(hit);
  }

  public static CoreSearchRow fromResponse(ObjectNode hit) {
    String index = hit.get("index").textValue();
    String id = hit.get("id").textValue();
    double score = hit.get("score").doubleValue();
    ObjectNode explanationJson = (ObjectNode) hit.get("explanation");

    Optional<CoreSearchRowLocations> locations = Optional.ofNullable(hit.get("locations"))
            .map(v -> CoreSearchRowLocations.from((ObjectNode) v));

    ObjectNode fragmentsJson = (ObjectNode) hit.get("fragments");
    final Map<String, List<String>> fragments;
    if (fragmentsJson != null) {
      fragments = new HashMap<>(fragmentsJson.size());
      fragmentsJson.fieldNames().forEachRemaining(field -> {
        ArrayNode fragmentJson = (ArrayNode) fragmentsJson.get(field);
        List<String> fragment = fragmentJson == null
                ? Collections.emptyList()
                : Mapper.convertValue(fragmentJson, new TypeReference<List<String>>() {});
        fragments.put(field, fragment);
      });
    } else {
      fragments = Collections.emptyMap();
    }

    byte[] fields = null;
    if (hit.has("fields")) {
      fields = hit.get("fields").toString().getBytes(UTF_8);
    }
    return new CoreSearchRow(index, id, score, explanationJson, locations, fragments, fields);
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
            '}';
  }
}

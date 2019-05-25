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

package com.couchbase.client.java.manager;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.SearchServiceException;
import com.couchbase.client.java.json.JacksonTransformers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class SearchIndex {

  private final ObjectNode rootNode;

  /**
   * Creates another index from the given one with its properties, but clears out all state like the
   * UUID so it can be used to create a new one.
   *
   * @param name the new name of the index
   * @param index the original index to copy the properties over
   * @return a new index that can be used
   */
  public static SearchIndex from(final String name, final SearchIndex index) {
    return new SearchIndex(index).name(name).clearUuid();
  }

  private SearchIndex(SearchIndex other) {
    this.rootNode = other.rootNode.deepCopy();
  }

  private SearchIndex(byte[] raw) {
    try {

      JsonNode node = JacksonTransformers.MAPPER.readTree(raw);
      if (node.has("indexDef")) {
        node = node.path("indexDef");
      }
      this.rootNode = (ObjectNode) node;
    } catch (IOException e) {
      throw new SearchServiceException("Could not decode index definition!", e);
    }
  }

  private  SearchIndex name(final String name) {
    rootNode.put("name", name);
    return this;
  }

  private SearchIndex clearUuid() {
    rootNode.remove("uuid");
    return this;
  }

  public Optional<String> uuid() {
    try {
      if (!rootNode.has("uuid")) {
        return Optional.empty();
      }
      return Optional.of(JacksonTransformers.MAPPER.treeToValue(rootNode.path("uuid"), String.class));
    } catch (JsonProcessingException e) {
      throw new SearchServiceException("Could not decode index definition!", e);
    }
  }

  public static SearchIndex fromJson(byte[] raw) {
    return new SearchIndex(raw);
  }

  public byte[] toJson() {
    try {
      return JacksonTransformers.MAPPER.writeValueAsBytes(rootNode);
    } catch (JsonProcessingException e) {
      throw new SearchServiceException("Could not encode index definition!", e);
    }
  }

  public String name() {
    try {
      return JacksonTransformers.MAPPER.treeToValue(rootNode.path("name"), String.class);
    } catch (JsonProcessingException e) {
      throw new SearchServiceException("Could not decode index definition!", e);
    }
  }

  @Override
  public String toString() {
    return "SearchIndex{" +
      "raw=" + new String(toJson(), StandardCharsets.UTF_8) +
      '}';
  }
}

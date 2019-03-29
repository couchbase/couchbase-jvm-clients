/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java.view;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.error.ViewServiceException;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

public class ViewRow {

  private final byte[] raw;
  private final JsonNode rootNode;

  public ViewRow(final byte[] raw) {
    this.raw = raw;
    try {
      this.rootNode = JacksonTransformers.MAPPER.readTree(raw);
    } catch (IOException e) {
      throw new ViewServiceException("Could not parse row!");
    }
  }

  public Optional<String> id() {
    return decode(String.class, "id");
  }

  public <T> Optional<T> keyAs(final Class<T> target) {
    return decode(target, "key");
  }

  public <T> Optional<T> valueAs(final Class<T> target) {
    return decode(target, "value");
  }

  public Optional<JsonObject> geometry() {
    return decode(JsonObject.class, "geometry");
  }

  private <T> Optional<T> decode(final Class<T> target, String path) {
    try {
      JsonNode subNode = rootNode.path(path);
      if (subNode == null || subNode.isNull() || subNode.isMissingNode()) {
        return Optional.empty();
      }
      return Optional.ofNullable(JacksonTransformers.MAPPER.treeToValue(subNode, target));
    } catch (JsonProcessingException e) {
      throw new ViewServiceException("Could not decode id in view row!");
    }
  }

  @Override
  public String toString() {
    return "ViewRow{" +
      "raw=" + new String(raw, StandardCharsets.UTF_8) +
      '}';
  }
}

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
import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.error.ViewServiceException;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

public class ViewRow {

  /**
   * Holds the raw, not yet decoded data of the response.
   */
  private final byte[] raw;

  /**
   * Provides a pointer into the root nodes of the raw response for easier decoding.
   */
  private final JsonNode rootNode;

  /**
   * Creates anew {@link ViewRow} from the raw row data.
   *
   * @param raw the raw json encoded row.
   */
  ViewRow(final byte[] raw) {
    this.raw = raw;
    try {
      this.rootNode = JacksonTransformers.MAPPER.readTree(raw);
    } catch (IOException e) {
      throw new ViewServiceException("Could not parse row!");
    }
  }

  /**
   * Returns the ID if present.
   *
   * @return the ID if present.
   */
  public Optional<String> id() {
    return decode(String.class, "id");
  }

  /**
   * Decodes the key into the given target type if present.
   *
   * @param target the target type.
   * @param <T> the generic type to decode into.
   * @return the decoded key, if present.
   */
  public <T> Optional<T> keyAs(final Class<T> target) {
    return decode(target, "key");
  }

  /**
   * Decodes the value into the given target type if present.
   *
   * @param target the target type.
   * @param <T> the generic type to decode into.
   * @return the decoded value, if present.
   */
  public <T> Optional<T> valueAs(final Class<T> target) {
    return decode(target, "value");
  }

  /**
   * Returns the geometry of your result if present in the response.
   *
   * @return the geometry if present.
   */
  public Optional<JsonObject> geometry() {
    return decode(JsonObject.class, "geometry");
  }

  /**
   * Helper method to turn a given path of the raw data into the target class.
   *
   * @param target the target class to decode into.
   * @param path the path of the raw json.
   * @param <T> the generic type to decide into.
   * @return the generic decoded object if present and not null.
   */
  private <T> Optional<T> decode(final Class<T> target, final String path) {
    try {
      JsonNode subNode = rootNode.path(path);
      if (subNode == null || subNode.isNull() || subNode.isMissingNode()) {
        return Optional.empty();
      }
      return Optional.ofNullable(JacksonTransformers.MAPPER.treeToValue(subNode, target));
    } catch (JsonProcessingException e) {
      throw new DecodingFailedException("Could not decode " + path +" in view row!");
    }
  }

  @Override
  public String toString() {
    return "ViewRow{" +
      "raw=" + new String(raw, StandardCharsets.UTF_8) +
      '}';
  }

}

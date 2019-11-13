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

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.error.ViewServiceException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.json.MapperException;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

public class ViewRow {

  /**
   * Holds the raw, not yet decoded data of the response.
   */
  private final byte[] raw;

  /**
   * Provides a pointer into the root nodes of the raw response for easier decoding.
   */
  private final JsonNode rootNode;

  private final JsonSerializer serializer;

  /**
   * Creates anew {@link ViewRow} from the raw row data.
   *
   * @param raw the raw json encoded row.
   */
  ViewRow(final byte[] raw, final JsonSerializer serializer) {
    this.raw = raw;
    this.serializer = serializer;
    try {
      this.rootNode = Mapper.decodeIntoTree(raw);
    } catch (MapperException e) {
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
   * Decodes the key into the given target type if present.
   *
   * @param target the target type.
   * @param <T> the generic type to decode into.
   * @return the decoded key, if present.
   */
  public <T> Optional<T> keyAs(final TypeRef<T> target) {
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
   * Decodes the value into the given target type if present.
   *
   * @param target the target type.
   * @param <T> the generic type to decode into.
   * @return the decoded value, if present.
   */
  public <T> Optional<T> valueAs(final TypeRef<T> target) {
    return decode(target, "value");
  }

  /**
   * Helper method to turn a given path of the raw data into the target class.
   *
   * @param target the target class to decode into.
   * @param path the path of the raw json.
   * @param <T> the generic type to decode into.
   * @return the generic decoded object if present and not null.
   */
  private <T> Optional<T> decode(final Class<T> target, final String path) {
    return findNonNullNode(path).map(subNode -> {
      // daschl: I know this is a bit wasteful, but key and value are sub-structures of the
      // overall result and to make use of the serializer we need to turn it into a byte array
      byte[] raw = Mapper.encodeAsBytes(subNode);
      return serializer.deserialize(target, raw);
    });
  }

  /**
   * Helper method to turn a given path of the raw data into the target type.
   *
   * @param target the target type to decode into.
   * @param path the path of the raw json.
   * @param <T> the generic type to decode into.
   * @return the generic decoded object if present and not null.
   */
  private <T> Optional<T> decode(final TypeRef<T> target, final String path) {
    return findNonNullNode(path).map(subNode -> {
      // daschl: I know this is a bit wasteful, but key and value are sub-structures of the
      // overall result and to make use of the serializer we need to turn it into a byte array
      byte[] raw = Mapper.encodeAsBytes(subNode);
      return serializer.deserialize(target, raw);
    });
  }

  private Optional<JsonNode> findNonNullNode(String path) {
    JsonNode subNode = rootNode.get(path);
    return subNode == null || subNode.isNull() ? Optional.empty() : Optional.of(subNode);
  }

  @Override
  public String toString() {
    return "ViewRow{" +
      "raw=" + redactUser(new String(raw, StandardCharsets.UTF_8)) +
      '}';
  }

}

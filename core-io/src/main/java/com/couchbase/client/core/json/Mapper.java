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

package com.couchbase.client.core.json;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectReader;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectWriter;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Provides utilities for encoding and decoding JSON data.
 *
 * @since 2.0.0
 */
@Stability.Internal
public class Mapper {

  private Mapper() {
    throw new AssertionError("not instantiable");
  }

  // ObjectMapper is mutable. To prevent it from being accidentally (or maliciously) reconfigured,
  // don't expose the ObjectMapper outside this class.
  private static final ObjectMapper mapper = new ObjectMapper();

  // Instead, expose immutable reader and writer for advanced use cases.
  private static final ObjectReader reader = mapper.reader();
  private static final ObjectWriter writer = mapper.writer();

  /**
   * Encodes the given input into a byte array, formatted non-pretty.
   *
   * @param input the java object as input
   * @return the json encoded byte array.
   */
  public static byte[] encodeAsBytes(final Object input) {
    try {
      return mapper.writeValueAsBytes(input);
    } catch (Exception ex) {
      throw new MapperException("Could not encode into JSON: " + redactUser(input), ex);
    }
  }

  /**
   * Encodes the given input into a byte array, formatted pretty.
   *
   * @param input the java object as input
   * @return the json encoded byte array.
   */
  public static byte[] encodeAsBytesPretty(final Object input) {
    try {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(input);
    } catch (Exception ex) {
      throw new MapperException("Could not encode into JSON: " + redactUser(input), ex);
    }
  }

  /**
   * Encodes the given input into a String, formatted non-pretty.
   *
   * @param input the java object as input
   * @return the json encoded String.
   */
  public static String encodeAsString(final Object input) {
    try {
      return mapper.writeValueAsString(input);
    } catch (Exception ex) {
      throw new MapperException("Could not encode into JSON: " + redactUser(input), ex);
    }
  }

  /**
   * Encodes the given input into a String, formatted pretty.
   *
   * @param input the java object as input
   * @return the json encoded String.
   */
  public static String encodeAsStringPretty(final Object input) {
    try {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(input);
    } catch (Exception ex) {
      throw new MapperException("Could not encode into JSON: " + redactUser(input), ex);
    }
  }

  /**
   * Decodes a byte array into the given class.
   *
   * @param input the input byte array.
   * @param clazz the clazz which should be decoded into.
   * @param <T> generic type used for inference.
   * @return the created instance.
   */
  public static <T> T decodeInto(byte[] input, Class<T> clazz) {
    try {
      return mapper.readValue(input, clazz);
    } catch (Exception ex) {
      throw new MapperException("Could not decode from JSON: " + redactUser(new String(input, UTF_8)), ex);
    }
  }

  /**
   * Decodes a String into the given class.
   *
   * @param input the input byte array.
   * @param clazz the clazz which should be decoded into.
   * @param <T> generic type used for inference.
   * @return the created instance.
   */
  public static <T> T decodeInto(String input, Class<T> clazz) {
    try {
      return mapper.readValue(input, clazz);
    } catch (Exception ex) {
      throw new MapperException("Could not decode from JSON: " + redactUser(input), ex);
    }
  }

  /**
   * Decodes a byte array into the given type.
   *
   * @param input the input byte array.
   * @param type the type which should be decoded into.
   * @param <T> generic type used for inference.
   * @return the created instance.
   */

  public static <T> T decodeInto(byte[] input, TypeReference<T> type) {
    try {
      return mapper.readValue(input, type);
    } catch (Exception ex) {
      throw new MapperException("Could not decode from JSON: " + redactUser(new String(input, UTF_8)), ex);
    }
  }

  /**
   * Decodes a String into the given type.
   *
   * @param input the input byte array.
   * @param type the type which should be decoded into.
   * @param <T> generic type used for inference.
   * @return the created instance.
   */

  public static <T> T decodeInto(String input, TypeReference<T> type) {
    try {
      return mapper.readValue(input, type);
    } catch (Exception ex) {
      throw new MapperException("Could not decode from JSON: " + redactUser(input), ex);
    }
  }

  /**
   * Decodes a byte array into a tree of JSON nodes.
   *
   * @param input the input byte array.
   * @return the created node.
   */
  public static JsonNode decodeIntoTree(byte[] input) {
    try {
      return mapper.readTree(input);
    } catch (Exception ex) {
      throw new MapperException("Could not decode from JSON: " + redactUser(new String(input, UTF_8)), ex);
    }
  }

  /**
   * Decodes a string into a tree of JSON nodes.
   *
   * @param input the input byte array.
   * @return the created node.
   */
  public static JsonNode decodeIntoTree(String input) {
    try {
      return mapper.readTree(input);
    } catch (Exception ex) {
      throw new MapperException("Could not decode from JSON: " + redactUser(input), ex);
    }
  }

  /**
   * Converts an object to the requested type using
   * {@link ObjectMapper#convertValue(Object, Class)}.
   */
  public static <T> T convertValue(Object fromValue, Class<T> toValueType) {
    return mapper.convertValue(fromValue, toValueType);
  }

  /**
   * Converts an object to the requested type using
   * {@link ObjectMapper#convertValue(Object, TypeReference)}.
   */
  public static <T> T convertValue(Object fromValue, TypeReference<T> toValueTypeRef) {
    return mapper.convertValue(fromValue, toValueTypeRef);
  }

  /**
   * Returns an ObjectReader for advanced use cases.
   */
  public static ObjectReader reader() {
    return reader;
  }

  /**
   * Returns an ObjectWriter for advanced use cases.
   */
  public static ObjectWriter writer() {
    return writer;
  }

  /**
   * Returns a new empty ObjectNode.
   */
  public static ObjectNode createObjectNode() {
    return mapper.createObjectNode();
  }

  /**
   * Returns a new empty ArrayNode.
   */
  public static ArrayNode createArrayNode() {
    return mapper.createArrayNode();
  }
}

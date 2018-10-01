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

import com.fasterxml.jackson.databind.ObjectMapper;

public class Mapper {

  private Mapper() {
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Encodes the given input into a byte array, formatted non-pretty.
   *
   * @param input the java object as input
   * @return the json encoded byte array.
   */
  public static byte[] encodeAsBytes(final Object input) {
    try {
      return MAPPER.writeValueAsBytes(input);
    } catch (Exception ex) {
      throw new MapperException("Could not encode into JSON: " + input, ex);
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
      return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(input);
    } catch (Exception ex) {
      throw new MapperException("Could not encode into JSON: " + input, ex);
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
      return MAPPER.writeValueAsString(input);
    } catch (Exception ex) {
      throw new MapperException("Could not encode into JSON: " + input, ex);
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
      return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(input);
    } catch (Exception ex) {
      throw new MapperException("Could not encode into JSON: " + input, ex);
    }
  }
}

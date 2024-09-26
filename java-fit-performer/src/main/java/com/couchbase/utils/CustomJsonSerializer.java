/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.utils;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonObject;

/**
 * CustomJsonSerializer provides a generic implementation of the JsonSerializer interface.
 * <p>
 * This serializer is designed to handle the conversion of Java objects to JSON format
 * and back, with an additional boolean flag (`Serialized`) that indicates whether
 * the object has been serialized. The flag is included in the JSON payload, making
 * it easy to track the serialization state of objects.
 * <p>
 * Use Cases:
 * <ul>
 *   <li>This serializer can be used in scenarios where you need to serialize and deserialize
 *       objects while keeping track of their serialization state.
 * </ul>
 * <p>
 * Limitations:
 * <ul>
 *   <li>The current implementation assumes that the input objects can be serialized into
 *       a JSON format using Jackson's ObjectMapper. Complex or non-standard objects may
 *       require additional handling.
 *   <li>The `deserialize` methods in this implementation modify the original JSON object
 *       by setting the `Serialized` flag to `false`, which might not be suitable for
 *       all use cases.
 * </ul>
 */
public class CustomJsonSerializer implements JsonSerializer {
  @Override
  public byte[] serialize(Object input) {
    try {
      String json = Mapper.writer().writeValueAsString(input);
      var obj = JsonObject.create().put("Serialized", true);
      return obj.toBytes();
    } catch (JsonProcessingException e) {
      throw new DecodingFailureException(e);
    }
  }

  @Override
  public <T> T deserialize(Class<T> target, byte[] input) {
    JsonObject obj = JsonObject.fromJson(input);
    obj.put("Serialized", false);
    return (T) obj;
  }

  @Override
  public <T> T deserialize(TypeRef<T> target, byte[] input) {
    JsonObject obj = JsonObject.fromJson(input);
    obj.put("Serialized", false);
    return (T) obj;
  }
}

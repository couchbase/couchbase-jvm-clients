/*
 * Copyright (c) 2024 Couchbase, Inc.
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

package com.couchbase.columnar.util;

import com.couchbase.columnar.client.java.codec.Deserializer;
import com.couchbase.columnar.client.java.codec.TypeRef;
import com.couchbase.columnar.client.java.json.JsonObject;

/**
 * CustomJsonDeserializer provides a generic implementation of the Deserializer interface.
 * <p>
 * This deserializer is designed to handle the conversion of Java objects to String format
 * and back, with an additional boolean flag ("Serialized": false) that indicates whether
 * the object has been deserialized. The flag is included in the JSON payload and then
 * converted to string, making it easy to track the deserialization state of objects.
 * <p>
 * Use Cases:
 * - This deserializer can be used in scenarios where you need to deserialize
 * objects while keeping track of their deserialization state.
 * <p>
 * Limitations:
 * - The current implementation assumes that the input objects can be deserialized into
 * string format. Complex or non-standard objects may require additional handling.
 * - The `deserialize` methods in this implementation modify the original JSON object
 * by setting the `Serialized` flag to `false`, which might not be suitable for
 * all use cases.
 */

public class CustomDeserializer implements Deserializer {
  @Override
  public <T> T deserialize(Class<T> target, byte[] input) {
    JsonObject obj = JsonObject.fromJson(input);
    obj.put("Serialized", false);
    return (T) obj.toString();
  }

  @Override
  public <T> T deserialize(TypeRef<T> target, byte[] input) {
    JsonObject obj = JsonObject.fromJson(input);
    obj.put("Serialized", false);
    return (T) obj.toString();
  }
}

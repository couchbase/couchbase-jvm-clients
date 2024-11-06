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

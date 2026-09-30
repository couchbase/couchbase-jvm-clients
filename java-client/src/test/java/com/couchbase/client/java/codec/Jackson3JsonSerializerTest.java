/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.codec;


import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.java.encryption.FakeCryptoManager;
import com.couchbase.client.java.encryption.annotation.Encrypted;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledForJreRange(
  min = JRE.JAVA_8,
  max = JRE.JAVA_16,
  disabledReason = "Jackson 3 requires Java 17 or later."
)
class Jackson3JsonSerializerTest extends JsonSerializerTestBase {
  private static final JsonSerializer serializer = Jackson3JsonSerializer.create();

  @Override
  protected JsonSerializer serializer() {
    return serializer;
  }

  @Nested
  class WrappedMapperWithoutModule extends JsonSerializerTestBase {
    private final JsonSerializer wrapped = new JsonValueSerializerWrapper(
      Jackson3TestSupport.serializerWithSharedMapper()
    );

    @Override
    protected JsonSerializer serializer() {
      return wrapped;
    }
  }

  @Test
  void canUseDataBinding() {
    Thing thing = new Thing();
    thing.name = "foo";

    byte[] jsonBytes = serializer.serialize(thing);
    assertEquals("{\"n\":\"foo\"}", new String(jsonBytes, UTF_8));

    thing = serializer.deserialize(Thing.class, jsonBytes);
    assertEquals("foo", thing.name);

    thing = serializer.deserialize(new TypeRef<Thing>() {}, jsonBytes);
    assertEquals("foo", thing.name);
  }

  public static class Thing {
    @JsonProperty("n")
    public String name;
  }

  @Test
  void createWithCryptoManagerEncryptsAnnotatedFields() {
    JsonSerializer cryptoSerializer = Jackson3JsonSerializer.create(new FakeCryptoManager());

    SecretThing thing = new SecretThing();
    thing.name = "foo";
    thing.secret = "bar";

    byte[] jsonBytes = cryptoSerializer.serialize(thing);
    Jackson3TestSupport.assertJsonEquals(
      "{\"name\":\"foo\",\"encrypted$secret\":{\"alg\":\"FAKE\",\"ciphertext\":\"ImJhciI=\"}}",
      jsonBytes
    );

    thing = cryptoSerializer.deserialize(SecretThing.class, jsonBytes);
    assertEquals("foo", thing.name);
    assertEquals("bar", thing.secret);
  }

  @Test
  void createWithoutCryptoManagerIgnoresEncryptedAnnotation() {
    SecretThing thing = new SecretThing();
    thing.name = "foo";
    thing.secret = "bar";

    byte[] jsonBytes = Jackson3JsonSerializer.create().serialize(thing);
    Jackson3TestSupport.assertJsonEquals(
      "{\"name\":\"foo\",\"secret\":\"bar\"}",
      jsonBytes
    );
  }

  public static class SecretThing {
    public String name;

    @Encrypted
    public String secret;
  }

  private static final String JSON_VALUES_JSON = "{"
    + "\"obj\":{\"city\":\"Paris\",\"price\":1.5,\"none\":null,"
    + "\"nested\":{\"zip\":75001},\"tags\":[\"a\",1,true],\"map\":{\"k\":\"v\"},\"list\":[1,2]},"
    + "\"arr\":[\"x\",{\"a\":1},[2,3],[\"l\"],{\"m\":false}]"
    + "}";

  public static class JsonValues {
    public JsonObject obj;
    public JsonArray arr;
  }

  private static JsonValues newJsonValues() {
    JsonValues values = new JsonValues();
    values.obj = JsonObject.create()
      .put("city", "Paris")
      .put("price", 1.5)
      .putNull("none")
      .put("nested", JsonObject.create().put("zip", 75001))
      .put("tags", JsonArray.from("a", 1, true))
      .put("map", mapOf("k", "v"))
      .put("list", listOf(1, 2));
    values.arr = JsonArray.from(
      "x",
      JsonObject.create().put("a", 1),
      JsonArray.from(2, 3),
      listOf("l"),
      mapOf("m", false)
    );
    return values;
  }

  private static void assertJsonValuesRoundTrip(JsonSerializer s) {
    JsonValues values = newJsonValues();

    byte[] jsonBytes = s.serialize(values);
    Jackson3TestSupport.assertJsonEquals(JSON_VALUES_JSON, jsonBytes);

    JsonValues decoded = s.deserialize(JsonValues.class, JSON_VALUES_JSON.getBytes(UTF_8));
    assertEquals(values.obj, decoded.obj);
    assertEquals(values.arr, decoded.arr);

    decoded = s.deserialize(new TypeRef<JsonValues>() {}, jsonBytes);
    assertEquals(values.obj, decoded.obj);
    assertEquals(values.arr, decoded.arr);
  }

  @Test
  void createRegistersJsonValueModule() {
    assertJsonValuesRoundTrip(Jackson3JsonSerializer.create());
  }

  @Test
  void createWithCryptoManagerRegistersJsonValueModule() {
    assertJsonValuesRoundTrip(Jackson3JsonSerializer.create(new FakeCryptoManager()));
  }

  @Test
  void mapperWithJsonValueModuleHandlesJsonValueFields() {
    assertJsonValuesRoundTrip(Jackson3TestSupport.serializerWithJsonValueModule());
  }

  @Test
  void clusterEnvironmentSerializerHandlesJsonValueFields() {
    try (ClusterEnvironment env = ClusterEnvironment.builder()
      .jsonSerializer(Jackson3JsonSerializer.create())
      .build()) {
      assertJsonValuesRoundTrip(env.jsonSerializer());
    }
  }

  @Test
  void jsonValueModuleReportsUnexpectedToken() {
    DecodingFailureException e = assertThrows(DecodingFailureException.class, () ->
      serializer.deserialize(JsonValues.class, "{\"obj\":[1]}".getBytes(UTF_8)));
    assertTrue(e.getCause().getMessage().startsWith("Expected START_OBJECT but got START_ARRAY"), e.getCause().getMessage());
  }

  @Test
  void canEncryptJsonObjectField() {
    JsonSerializer cryptoSerializer = Jackson3JsonSerializer.create(new FakeCryptoManager());

    SecretJsonObject thing = new SecretJsonObject();
    thing.secret = JsonObject.create().put("nested", JsonObject.create().put("a", 1));

    byte[] jsonBytes = cryptoSerializer.serialize(thing);
    Jackson3TestSupport.assertJsonEquals(
      "{\"encrypted$secret\":{\"alg\":\"FAKE\",\"ciphertext\":\"eyJuZXN0ZWQiOnsiYSI6MX19\"}}",
      jsonBytes
    );

    SecretJsonObject decoded = cryptoSerializer.deserialize(SecretJsonObject.class, jsonBytes);
    assertEquals(thing.secret, decoded.secret);
  }

  public static class SecretJsonObject {
    @Encrypted
    public JsonObject secret;
  }
}

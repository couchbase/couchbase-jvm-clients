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


import com.couchbase.client.java.encryption.FakeCryptoManager;
import com.couchbase.client.java.encryption.annotation.Encrypted;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

@DisabledForJreRange(
  min = JRE.JAVA_8,
  max = JRE.JAVA_16,
  disabledReason = "Jackson 3 requires Java 17 or later."
)
class Jackson3JsonSerializerTest extends JsonSerializerTestBase {
  private static final JsonSerializer serializer = new JsonValueSerializerWrapper(
    Jackson3TestSupport.serializerWithSharedMapper()
  );

  @Override
  protected JsonSerializer serializer() {
    return serializer;
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
}

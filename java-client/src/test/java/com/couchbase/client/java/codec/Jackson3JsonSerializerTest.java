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


import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import tools.jackson.databind.json.JsonMapper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

@DisabledForJreRange(
  min = JRE.JAVA_8,
  max = JRE.JAVA_16,
  disabledReason = "Jackson 3 requires Java 17 or later."
)
class Jackson3JsonSerializerTest extends JsonSerializerTestBase {
  private static final JsonSerializer serializer = new JsonValueSerializerWrapper(
    Jackson3JsonSerializer.create(JsonMapper.shared())
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
}

/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.columnar.client.java.internal;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonGenerator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonToken;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.Version;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DeserializationContext;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonDeserializer;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonSerializer;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.SerializerProvider;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.module.SimpleModule;
import com.couchbase.columnar.client.java.json.JsonArray;
import com.couchbase.columnar.client.java.json.JsonObject;
import reactor.util.annotation.Nullable;

import java.io.IOException;

@Stability.Internal
public class RepackagedJsonValueModule extends SimpleModule {

  public RepackagedJsonValueModule() {
    super(new Version(1, 0, 0, null, "com.couchbase", "JsonValueModule"));

    addSerializer(JsonObject.class, new JsonObjectSerializer());
    addDeserializer(JsonObject.class, new JsonObjectDeserializer());

    addSerializer(JsonArray.class, new JsonArraySerializer());
    addDeserializer(JsonArray.class, new JsonArrayDeserializer());
  }

  static class JsonObjectSerializer extends JsonSerializer<JsonObject> {
    @Override
    public void serialize(JsonObject value, JsonGenerator generator,
                          SerializerProvider provider) throws IOException {
      generator.writeObject(value.toMap());
    }
  }

  static class JsonArraySerializer extends JsonSerializer<JsonArray> {
    @Override
    public void serialize(JsonArray value, JsonGenerator generator,
                          SerializerProvider provider) throws IOException {
      generator.writeObject(value.toList());
    }
  }

  abstract static class AbstractJsonValueDeserializer<T> extends JsonDeserializer<T> {

    JsonObject decodeObject(final JsonParser parser) throws IOException {
      expectCurrentToken(parser, JsonToken.START_OBJECT);

      final JsonObject result = JsonObject.create();
      while (true) {
        final JsonToken current = parser.nextToken();
        if (current == JsonToken.END_OBJECT) {
          return result;
        }
        expectCurrentToken(parser, JsonToken.FIELD_NAME);
        parser.nextToken(); // consume field name
        result.put(parser.currentName(), decodeValue(parser));
      }
    }

    JsonArray decodeArray(final JsonParser parser) throws IOException {
      expectCurrentToken(parser, JsonToken.START_ARRAY);

      final JsonArray result = JsonArray.create();
      while (true) {
        final JsonToken current = parser.nextToken();
        if (current == JsonToken.END_ARRAY) {
          return result;
        }
        result.add(decodeValue(parser));
      }
    }

    @Nullable
    Object decodeValue(final JsonParser parser) throws IOException {
      final JsonToken current = currentToken(parser);
      switch (current) {
        case START_OBJECT:
          return decodeObject(parser);
        case START_ARRAY:
          return decodeArray(parser);
        case VALUE_TRUE:
        case VALUE_FALSE:
          return parser.getBooleanValue();
        case VALUE_STRING:
          return parser.getValueAsString();
        case VALUE_NUMBER_INT:
        case VALUE_NUMBER_FLOAT:
          return parser.getNumberValue();
        case VALUE_NULL:
          return null;
        default:
          throw new IOException("Unexpected JSON token: " + current);
      }
    }
  }

  static class JsonArrayDeserializer extends AbstractJsonValueDeserializer<JsonArray> {
    @Override
    public JsonArray deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {
      return decodeArray(jp);
    }
  }

  static class JsonObjectDeserializer extends AbstractJsonValueDeserializer<JsonObject> {
    @Override
    public JsonObject deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {
      return decodeObject(jp);
    }
  }

  private static void expectCurrentToken(final JsonParser parser, JsonToken expected) throws IOException {
    if (currentToken(parser) != expected) {
      throw new IOException("Expected " + expected + " but got " + currentToken(parser));
    }
  }

  private static JsonToken currentToken(JsonParser parser) {
    // For compatibility with ancient versions of Jackson 2 provided by the user,
    // call getCurrentToken() instead of currentToken().
    // Only relevant in the non-Repackaged flavor of this class, but for consistency
    // do the same thing in both flavors.
    return parser.getCurrentToken();
  }

}

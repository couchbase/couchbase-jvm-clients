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

package com.couchbase.client.java.json;

// CHECKSTYLE:OFF IllegalImport - Allow unbundled Jackson

import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.Version;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.module.SimpleModule;

/**
 * Can be registered with a Jackson 3 {@code JsonMapper}
 * to add support for Couchbase {@link JsonObject} and {@link JsonArray}.
 * <p>
 * Example usage:
 * <pre>
 * JsonMapper mapper = JsonMapper.builder()
 *     .addModule(new Jackson3JsonValueModule())
 *     .build();
 * </pre>
 *
 * @implNote Keep in sync with {@link JsonValueModule} and {@link RepackagedJsonValueModule}.
 */
public class Jackson3JsonValueModule extends SimpleModule {

  private final boolean decimalForFloat = Boolean.parseBoolean(
      System.getProperty("com.couchbase.json.decimalForFloat", "false"));

  public Jackson3JsonValueModule() {
    super(new Version(1, 0, 0, null, "com.couchbase", "JsonValueModule"));

    addSerializer(JsonObject.class, new JsonObjectSerializer());
    addDeserializer(JsonObject.class, new JsonObjectDeserializer());

    addSerializer(JsonArray.class, new JsonArraySerializer());
    addDeserializer(JsonArray.class, new JsonArrayDeserializer());
  }

  static class JsonObjectSerializer extends ValueSerializer<JsonObject> {
    @Override
    public void serialize(JsonObject value, JsonGenerator gen, SerializationContext ctxt) {
      ctxt.writeValue(gen, value.toMap());
    }
  }

  static class JsonArraySerializer extends ValueSerializer<JsonArray> {
    @Override
    public void serialize(JsonArray value, JsonGenerator gen, SerializationContext ctxt) {
      ctxt.writeValue(gen, value.toList());
    }
  }

  abstract class AbstractJsonValueDeserializer<T> extends ValueDeserializer<T> {

    JsonObject decodeObject(final JsonParser parser, final DeserializationContext ctxt) {
      expectCurrentToken(parser, ctxt, JsonToken.START_OBJECT);

      final JsonObject result = JsonObject.create();
      while (true) {
        final JsonToken current = parser.nextToken();
        if (current == JsonToken.END_OBJECT) {
          return result;
        }
        expectCurrentToken(parser, ctxt, JsonToken.PROPERTY_NAME);
        parser.nextToken(); // consume property name
        result.put(parser.currentName(), decodeValue(parser, ctxt));
      }
    }

    JsonArray decodeArray(final JsonParser parser, final DeserializationContext ctxt) {
      expectCurrentToken(parser, ctxt, JsonToken.START_ARRAY);

      final JsonArray result = JsonArray.create();
      while (true) {
        final JsonToken current = parser.nextToken();
        if (current == JsonToken.END_ARRAY) {
          return result;
        }
        result.add(decodeValue(parser, ctxt));
      }
    }

    Object decodeValue(final JsonParser parser, final DeserializationContext ctxt) {
      final JsonToken current = parser.currentToken();
      switch (current) {
        case START_OBJECT:
          return decodeObject(parser, ctxt);
        case START_ARRAY:
          return decodeArray(parser, ctxt);
        case VALUE_TRUE:
        case VALUE_FALSE:
          return parser.getBooleanValue();
        case VALUE_STRING:
          return parser.getValueAsString();
        case VALUE_NUMBER_INT:
        case VALUE_NUMBER_FLOAT:
          Number numberValue = parser.getNumberValue();
          if (numberValue instanceof Double && decimalForFloat) {
            numberValue = parser.getDecimalValue();
          }
          return numberValue;
        case VALUE_NULL:
          return null;
        default:
          return ctxt.reportInputMismatch(this, "Unexpected JSON token: %s", current);
      }
    }

    private void expectCurrentToken(final JsonParser parser, final DeserializationContext ctxt, JsonToken expected) {
      if (parser.currentToken() != expected) {
        ctxt.reportInputMismatch(this, "Expected %s but got %s", expected, parser.currentToken());
      }
    }
  }

  class JsonArrayDeserializer extends AbstractJsonValueDeserializer<JsonArray> {
    @Override
    public JsonArray deserialize(JsonParser jp, DeserializationContext ctxt) {
      return decodeArray(jp, ctxt);
    }
  }

  class JsonObjectDeserializer extends AbstractJsonValueDeserializer<JsonObject> {
    @Override
    public JsonObject deserialize(JsonParser jp, DeserializationContext ctxt) {
      return decodeObject(jp, ctxt);
    }
  }
}

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

package com.couchbase.client.java.encryption.databind.jackson3;

// CHECKSTYLE:OFF IllegalImport - Allow unbundled Jackson

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.java.encryption.annotation.Encrypted;
import tools.jackson.core.JsonParser;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.BeanProperty;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.ValueDeserializer;

import java.util.Map;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class EncryptedFieldDeserializer extends ValueDeserializer<Object> {

  private static final TypeReference<Map<String, Object>> MAP_STRING_OBJECT = new TypeReference<Map<String, Object>>() {
  };

  private final CryptoManager cryptoManager;
  private final Encrypted annotation;
  private final JavaType beanPropertyType;

  public EncryptedFieldDeserializer(CryptoManager cryptoManager, Encrypted annotation) {
    this(cryptoManager, annotation, new BeanProperty.Bogus());
  }

  public EncryptedFieldDeserializer(CryptoManager cryptoManager, Encrypted annotation, BeanProperty beanProperty) {
    this.cryptoManager = requireNonNull(cryptoManager);
    this.annotation = requireNonNull(annotation);
    this.beanPropertyType = beanProperty.getType();
  }

  /**
   * Returns a copy of this deserializer specialized for the given context.
   */
  @Override
  public ValueDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) {
    return new EncryptedFieldDeserializer(cryptoManager, annotation, property);
  }

  @Override
  public Object deserialize(JsonParser p, DeserializationContext ctxt) {
    final Map<String, Object> encrypted = ctxt.readValue(p, MAP_STRING_OBJECT);
    final byte[] plaintext = cryptoManager.decrypt(encrypted);

    try (JsonParser plaintextParser = ctxt.createParser(plaintext)) {
      return ctxt.readValue(plaintextParser, beanPropertyType);
    }
  }
}

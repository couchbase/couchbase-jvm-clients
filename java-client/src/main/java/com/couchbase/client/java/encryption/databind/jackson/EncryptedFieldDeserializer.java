/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.java.encryption.databind.jackson;

// CHECKSTYLE:OFF IllegalImport - Allow unbundled Jackson

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.java.encryption.annotation.EncryptedField;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class EncryptedFieldDeserializer extends JsonDeserializer<Object> implements ContextualDeserializer {

  private static final TypeReference<Map<String, Object>> MAP_STRING_OBJECT = new TypeReference<Map<String, Object>>() {
  };

  private final CryptoManager cryptoManager;
  private final EncryptedField annotation;
  private final TypeReference<?> beanPropertyTypeRef;

  public EncryptedFieldDeserializer(CryptoManager cryptoManager, EncryptedField annotation) {
    this(cryptoManager, annotation, new BeanProperty.Bogus());
  }

  public EncryptedFieldDeserializer(CryptoManager cryptoManager, EncryptedField annotation, BeanProperty beanProperty) {
    this.cryptoManager = requireNonNull(cryptoManager);
    this.annotation = requireNonNull(annotation);
    this.beanPropertyTypeRef = getTypeRef(beanProperty);
  }

  private static TypeReference<?> getTypeRef(BeanProperty beanProperty) {
    final JavaType propertyType = beanProperty.getType();
    return new TypeReference<Object>() {
      public Type getType() {
        return propertyType;
      }
    };
  }

  /**
   * Returns a copy of this deserializer specialized for the given context.
   */
  @Override
  public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) {
    return new EncryptedFieldDeserializer(cryptoManager, annotation, property);
  }

  @Override
  public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    final Map<String, Object> encrypted = p.readValueAs(MAP_STRING_OBJECT);
    final byte[] plaintext = cryptoManager.decrypt(encrypted);

    final JsonParser plaintextParser = p.getCodec().getFactory().createParser(plaintext);
    plaintextParser.setCodec(p.getCodec());

    return plaintextParser.readValueAs(beanPropertyTypeRef);
  }
}

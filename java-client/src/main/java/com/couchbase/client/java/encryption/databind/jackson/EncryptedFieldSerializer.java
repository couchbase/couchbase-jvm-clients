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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class EncryptedFieldSerializer extends StdSerializer<Object> {
  private final CryptoManager cryptoManager;
  private final EncryptedField annotation;
  private final JsonSerializer<Object> originalCustomSerializer; // nullable

  public EncryptedFieldSerializer(CryptoManager cryptoManager, EncryptedField annotation, JsonSerializer<Object> originalCustomSerializer) {
    super(Object.class);
    this.cryptoManager = requireNonNull(cryptoManager);
    this.annotation = requireNonNull(annotation);
    this.originalCustomSerializer = originalCustomSerializer; // nullable
  }

  @Override
  public void serialize(Object value, JsonGenerator gen, SerializerProvider provider) throws IOException {
    final byte[] plaintextJson = serializePlaintext(value, gen, provider);
    final Map<String, Object> encrypted = cryptoManager.encrypt(plaintextJson, annotation.encrypter());
    gen.writeObject(encrypted);
  }

  /**
   * Serializes the field value "normally" and returns the JSON bytes.
   */
  private byte[] serializePlaintext(Object value, JsonGenerator gen, SerializerProvider provider) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (JsonGenerator plaintextGenerator = gen.getCodec().getFactory().createGenerator(out)) {
      if (originalCustomSerializer != null) {
        originalCustomSerializer.serialize(value, plaintextGenerator, provider);
      } else {
        provider.defaultSerializeValue(value, plaintextGenerator);
      }
    }
    return out.toByteArray();
  }
}

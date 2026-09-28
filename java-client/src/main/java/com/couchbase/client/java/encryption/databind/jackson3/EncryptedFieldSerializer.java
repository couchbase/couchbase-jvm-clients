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
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.ser.std.StdSerializer;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class EncryptedFieldSerializer extends StdSerializer<Object> {
  private final CryptoManager cryptoManager;
  private final Encrypted annotation;
  private final ValueSerializer<Object> originalCustomSerializer; // nullable

  public EncryptedFieldSerializer(CryptoManager cryptoManager, Encrypted annotation, ValueSerializer<Object> originalCustomSerializer) {
    super(Object.class);
    this.cryptoManager = requireNonNull(cryptoManager);
    this.annotation = requireNonNull(annotation);
    this.originalCustomSerializer = originalCustomSerializer; // nullable
  }

  @Override
  public void serialize(Object value, JsonGenerator gen, SerializationContext ctxt) {
    final byte[] plaintextJson = serializePlaintext(value, ctxt);
    final Map<String, Object> encrypted = cryptoManager.encrypt(plaintextJson, annotation.encrypter());
    ctxt.writeValue(gen, encrypted);
  }

  /**
   * Serializes the field value "normally" and returns the JSON bytes.
   */
  private byte[] serializePlaintext(Object value, SerializationContext ctxt) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (JsonGenerator plaintextGenerator = ctxt.createGenerator(out)) {
      if (originalCustomSerializer != null) {
        originalCustomSerializer.serialize(value, plaintextGenerator, ctxt);
      } else {
        ctxt.writeValue(plaintextGenerator, value);
      }
    }
    return out.toByteArray();
  }
}

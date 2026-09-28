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
import tools.jackson.core.io.SerializedString;
import tools.jackson.databind.BeanDescription;
import tools.jackson.databind.SerializationConfig;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.ser.BeanPropertyWriter;
import tools.jackson.databind.ser.ValueSerializerModifier;

import java.util.ArrayList;
import java.util.List;

import static com.couchbase.client.java.encryption.databind.jackson3.EncryptionModule.findAnnotation;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class EncryptedFieldSerializationModifier extends ValueSerializerModifier {
  private final CryptoManager cryptoManager;

  public EncryptedFieldSerializationModifier(CryptoManager cryptoManager) {
    this.cryptoManager = requireNonNull(cryptoManager);
  }

  @Override
  public List<BeanPropertyWriter> changeProperties(SerializationConfig config,
                                                   BeanDescription.Supplier beanDesc,
                                                   List<BeanPropertyWriter> beanProperties) {
    final List<BeanPropertyWriter> result = new ArrayList<>();
    for (BeanPropertyWriter writer : beanProperties) {
      final Encrypted annotation = findAnnotation(writer, Encrypted.class);
      result.add(annotation == null ? writer : new EncryptedBeanPropertyWriter(writer, annotation));
    }
    return result;
  }

  private class EncryptedBeanPropertyWriter extends BeanPropertyWriter {
    EncryptedBeanPropertyWriter(BeanPropertyWriter original, Encrypted annotation) {
      // init with same values as original, and mangled name
      super(original, new SerializedString(cryptoManager.mangle(original.getName())));

      final ValueSerializer<Object> newSerializer =
          new EncryptedFieldSerializer(cryptoManager, annotation, original.getSerializer());

      // clobber existing serializers (can't call assignSerializer() because that fails if one is already set)
      _serializer = newSerializer;
      _nullSerializer = newSerializer;
    }
  }
}

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
import com.couchbase.client.core.util.CbAnnotations;
import com.couchbase.client.java.encryption.annotation.Encrypted;
import tools.jackson.core.Version;
import tools.jackson.databind.BeanProperty;
import tools.jackson.databind.JacksonModule;

import java.lang.annotation.Annotation;
import java.util.Iterator;

import static java.util.Objects.requireNonNull;

/**
 * Can be registered with a Jackson 3 {@code JsonMapper} to activate
 * the {@link Encrypted} annotation.
 * <p>
 * Example usage:
 * <pre>
 * JsonMapper mapper = JsonMapper.builder()
 *     .addModule(new EncryptionModule(cryptoManager))
 *     .build();
 * </pre>
 */
@Stability.Volatile
public class EncryptionModule extends JacksonModule {
  private final CryptoManager cryptoManager;

  public EncryptionModule(CryptoManager cryptoManager) {
    this.cryptoManager = requireNonNull(cryptoManager);
  }

  @Override
  public String getModuleName() {
    return "CouchbaseEncryption";
  }

  @Override
  public Version version() {
    return new Version(1, 0, 0, null, "com.couchbase", getModuleName());
  }

  @Override
  public void setupModule(SetupContext context) {
    context.addSerializerModifier(new EncryptedFieldSerializationModifier(cryptoManager));
    context.addDeserializerModifier(new EncryptedFieldDeserializationModifier(cryptoManager));
  }

  /**
   * Like {@link BeanProperty#getAnnotation(Class)}, but searches for meta-annotations as well.
   */
  static <T extends Annotation> T findAnnotation(BeanProperty prop, Class<T> annotationClass) {
    Iterator<Annotation> annotations = prop.getMember().annotations().iterator();
    while (annotations.hasNext()) {
      T match = CbAnnotations.findAnnotation(annotations.next(), annotationClass);
      if (match != null) {
        return match;
      }
    }
    return null;
  }
}

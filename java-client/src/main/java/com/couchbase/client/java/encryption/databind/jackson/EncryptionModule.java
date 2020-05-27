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
import com.couchbase.client.core.util.CbAnnotations;
import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.java.encryption.annotation.Encrypted;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.Module;

import java.lang.annotation.Annotation;

import static java.util.Objects.requireNonNull;

/**
 * Can be registered with a Jackson {@code ObjectMapper} to activate
 * the {@link Encrypted} annotation.
 */
@Stability.Volatile
public class EncryptionModule extends Module {
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
    context.addBeanSerializerModifier(new EncryptedFieldSerializationModifier(cryptoManager));
    context.addBeanDeserializerModifier(new EncryptedFieldDeserializationModifier(cryptoManager));
  }

  /**
   * Like {@link BeanProperty#getAnnotation(Class)}, but searches for meta-annotations as well.
   */
  static <T extends Annotation> T findAnnotation(BeanProperty prop, Class<T> annotationClass) {
    for (Annotation a : prop.getMember().getAllAnnotations().annotations()) {
      T match = CbAnnotations.findAnnotation(a, annotationClass);
      if (match != null) {
        return match;
      }
    }
    return null;
  }
}

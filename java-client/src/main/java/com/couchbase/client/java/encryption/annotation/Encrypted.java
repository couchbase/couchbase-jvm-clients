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

package com.couchbase.client.java.encryption.annotation;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.encryption.CryptoManager;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Indicates the annotated field should be encrypted during serialization
 * and decrypted during deserialization.
 * <p>
 * May be used as a meta-annotation.
 *
 * @see com.couchbase.client.java.encryption.databind.jackson.EncryptionModule
 * @see com.couchbase.client.java.encryption.databind.jackson.repackaged.RepackagedEncryptionModule
 */
@Stability.Volatile
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD, ElementType.FIELD})
public @interface Encrypted {
  /**
   * Alias of encrypter to use.
   * <p>
   * May be omitted if the crypto manager has a default encrypter.
   */
  String encrypter() default CryptoManager.DEFAULT_ENCRYPTER_ALIAS;

  /**
   * Controls deserialization behavior when the provided field value
   * is not encrypted.
   * <p>
   * Defaults to {@link Migration#NONE}.
   *
   * @since 3.2.1
   */
  @Stability.Volatile
  Migration migration() default Migration.NONE;

  @Stability.Volatile
  enum Migration {
    /**
     * During deserialization, accept an unencrypted value if an encrypted value is not present.
     * This enables seamless migration when adding encryption to an existing field.
     * The drawback is that it removes any guarantee the field was written by someone
     * in possession of the secret key, since encryption implies authentication.
     */
    FROM_UNENCRYPTED,

    /**
     * During deserialization, when an unencrypted value is encountered it will either be ignored
     * or cause data binding to fail (depending on other deserialization settings).
     */
    NONE,
  }
}

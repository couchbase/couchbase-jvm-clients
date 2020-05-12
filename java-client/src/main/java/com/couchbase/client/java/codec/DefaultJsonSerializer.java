/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.codec;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JavaType;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.java.json.RepackagedJsonValueModule;
import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.java.encryption.databind.jackson.repackaged.RepackagedEncryptionModule;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The default JSON serializer.
 *
 * @implNote The serializer is backed by a repackaged version of Jackson,
 * but this is an implementation detail that users should not depend on.
 * <p>
 * Be aware that this serializer does not recognize standard Jackson annotations.
 * @see JacksonJsonSerializer
 */
public class DefaultJsonSerializer implements JsonSerializer {

  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * Creates an instance without encryption support.
   */
  public static DefaultJsonSerializer create() {
    return create(null);
  }

  /**
   * Creates an instance with optional encryption support.
   *
   * @param cryptoManager (nullable) The manager to use for activating the
   * {@code EncryptedField} annotation, or null to disable encryption support.
   */
  public static DefaultJsonSerializer create(CryptoManager cryptoManager) {
    return new DefaultJsonSerializer(cryptoManager);
  }

  private DefaultJsonSerializer(CryptoManager cryptoManager) {
    mapper.registerModule(new RepackagedJsonValueModule());
    if (cryptoManager != null) {
      mapper.registerModule(new RepackagedEncryptionModule(cryptoManager));
    }
  }

  @Override
  public byte[] serialize(final Object input) {
    if (input instanceof byte[]) {
      return (byte[]) input;
    }

    try {
      return mapper.writeValueAsBytes(input);
    } catch (Throwable t) {
      throw new EncodingFailureException("Serializing of content + " + redactUser(input) + " to JSON failed.", t);
    }
  }

  @Override
  public <T> T deserialize(final Class<T> target, final byte[] input) {
    if (target.isAssignableFrom(byte[].class)) {
      return (T) input;
    }

    try {
      return mapper.readValue(input, target);
    } catch (Throwable e) {
      throw new DecodingFailureException("Deserialization of content into target " + target
          + " failed; encoded = " + redactUser(new String(input, UTF_8)), e);
    }
  }

  @Override
  public <T> T deserialize(final TypeRef<T> target, final byte[] input) {
    try {
      JavaType type = mapper.getTypeFactory().constructType(target.type());
      return mapper.readValue(input, type);
    } catch (Throwable e) {
      throw new DecodingFailureException("Deserialization of content into target " + target
          + " failed; encoded = " + redactUser(new String(input, UTF_8)), e);
    }
  }

}

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

import com.couchbase.client.core.api.kv.CoreEncodedContent;
import com.couchbase.client.core.error.DecodingFailureException;

/**
 * The transcoder is responsible for transcoding KV binary packages between their binary and their java object
 * representation.
 */
public interface Transcoder {

  /**
   * Encodes the given input into the wire representation based on the data format.
   *
   * @param input the input object to encode.
   * @return the encoded wire representation of the payload.
   */
  EncodedValue encode(Object input);

  /**
   * Decodes the wire representation into the entity based on the data format.
   *
   * @param target the target type to decode.
   * @param input the wire representation to decode.
   * @param flags the flags on the wire
   * @param <T> the generic type used for the decoding target.
   * @return the decoded entity.
   */
  <T> T decode(Class<T> target, byte[] input, int flags);

  /**
   * Decodes the wire representation into the entity based on the data format.
   *
   * @param target the target type to decode.
   * @param input the wire representation to decode.
   * @param flags the flags on the wire
   * @param <T> the generic type used for the decoding target.
   * @return the decoded entity.
   */
  default <T> T decode(TypeRef<T> target, byte[] input, int flags) {
    throw new DecodingFailureException(getClass().getSimpleName() + " does not support decoding via TypeRef.");
  }

  /**
   * Represents the tuple of encoded value and flags to be used on the wire.
   */
  class EncodedValue implements CoreEncodedContent {

    private final byte[] encoded;
    private final int flags;

    public EncodedValue(final byte[] encoded, final int flags) {
      this.encoded = encoded;
      this.flags = flags;
    }

    public byte[] encoded() {
      return encoded;
    }

    public int flags() {
      return flags;
    }

  }

}

/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import com.couchbase.client.core.error.EncodingFailedException;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.kv.EncodedDocument;

/**
 * The default encoder used which registers common types and knows how to encode them
 * into their binary representation with the proper flags.
 *
 * @since 3.0.0
 */
public class DefaultEncoder implements Encoder {

  /**
   * The default instance to reuse.
   */
  public static final DefaultEncoder INSTANCE = new DefaultEncoder();

  @Override
  public EncodedDocument encode(final Object input) {
    if (input instanceof CommonOptions.BuiltCommonOptions || input instanceof CommonOptions) {
      throw new IllegalArgumentException("No content provided, cannot " +
        "encode the Options as content!");
    }

    try {
      int flags = Encoder.JSON_FLAGS;
      byte[] encoded;
      if (input instanceof TypedContent) {
        flags = ((TypedContent) input).flags();
        encoded = ((TypedContent) input).content();
      } else {
        encoded = JacksonTransformers.MAPPER.writeValueAsBytes(input);
      }
      return EncodedDocument.of(flags, encoded);
    } catch (Exception e) {
      throw new EncodingFailedException(e);
    }
  }
}

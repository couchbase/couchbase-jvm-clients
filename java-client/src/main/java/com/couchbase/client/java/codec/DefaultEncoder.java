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

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.kv.EncodedDocument;

public class DefaultEncoder implements Encoder {

  public static final DefaultEncoder INSTANCE = new DefaultEncoder();

  @Override
  public EncodedDocument encode(final Object input) {
    try {
      int flags = 0; // TODO: FIXME
      byte[] encoded;
      if (input instanceof BinaryContent) {
        encoded = ((BinaryContent) input).content();
      } else {
        encoded = JacksonTransformers.MAPPER.writeValueAsBytes(input); // FIXME
      }
      return new EncodedDocument(flags, encoded);
    } catch (Exception ex) {
      // TODO: better hierachy
      throw new CouchbaseException(ex);
    }
  }
}

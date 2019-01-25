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
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.kv.EncodedDocument;

import java.io.IOException;

public class DefaultDecoder implements Decoder<Object> {

  public static final DefaultDecoder INSTANCE = new DefaultDecoder();

  @Override
  public Object decode(Class<Object> target, EncodedDocument encoded) {
    try {
      if (target.isAssignableFrom(BinaryContent.class)) {
        return BinaryContent.wrap(encoded.content());
      } else {
        return JacksonTransformers.MAPPER.readValue(encoded.content(), target);
      }
    } catch (IOException e) {
      // TODO
      throw new CouchbaseException(e);
    }
  }

}

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
package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.PooledByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.java.codec.JsonSerializer;

import java.util.Iterator;
import java.util.List;

/**
 * Internal utility methods for the MutateIn subdoc operation.
 */
@Stability.Internal
public class MutateInUtil {

  private MutateInUtil() {}

  /**
   * Converts a list of objects (subdoc partials) into their low-level representation.
   * <p>
   * Per KV protocol each individual value is separated by a ",", which is not valid JSON on
   * the wire but the server will split it apart and apply each one of them individually.
   *
   * @param docs the docs to serialize.
   * @param serializer the serializer to use for JSON.
   * @return the serialized and converted byte array.
   */
  static byte[] convertDocsToBytes(final List<?> docs, final JsonSerializer serializer) {
    if (docs.size() == 1) {
      return serializer.serialize(docs.get(0));
    } else {
      final ByteBuf bytes = UnpooledByteBufAllocator.DEFAULT.heapBuffer();
      try {
        final Iterator<?> it = docs.iterator();
        while (it.hasNext()) {
          Object d = it.next();
          bytes.writeBytes(serializer.serialize(d));
          if (it.hasNext()) {
            bytes.writeByte(',');
          }
        }
        return ByteBufUtil.getBytes(bytes);
      } finally {
        bytes.release();
      }
    }
  }

}

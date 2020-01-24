/*
 * Copyright (c) 2020 Couchbase, Inc.
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

import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.msg.kv.CodecFlags;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * This transcoder allows to serialize and deserialize objects if they implement the {@link java.io.Serializable} interface.
 * <p>
 * Please note that this transcoder is NOT turning the value into a JSON representation, rather it is using the java
 * object serialization which will look to the server like an opaque binary blob. It is useful though if you want to
 * store and retrieve arbitrary java objects and use couchbase as a cache for them.
 */
public class SerializableTranscoder implements Transcoder {

  public static SerializableTranscoder INSTANCE = new SerializableTranscoder();

  private SerializableTranscoder() { }

  @Override
  public EncodedValue encode(Object input) {
    try {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();;
      final ObjectOutputStream os = new ObjectOutputStream(bos);
      os.writeObject(input);

      final byte[] serialized = bos.toByteArray();
      os.close();
      bos.close();
      return new EncodedValue(serialized, CodecFlags.SERIALIZED_COMPAT_FLAGS);
    } catch (Exception ex) {
      throw InvalidArgumentException.fromMessage("Could not encode (serialize) the given object!", ex);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T decode(Class<T> target, byte[] input, int flags) {
    try {
      final ByteArrayInputStream bis = new ByteArrayInputStream(input);
      final ObjectInputStream is = new ObjectInputStream(bis);
      final T deserialized = (T) is.readObject();
      is.close();
      bis.close();
      return deserialized;
    } catch (Exception ex) {
      throw InvalidArgumentException.fromMessage("Could not decode (deserialize) the given object!", ex);
    }
  }

}

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

import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.error.EncodingFailedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

/**
 * This serializer handles encoding and decoding from/to java object serialization.
 */
public class JavaObjectSerializer implements Serializer {

  public static final JavaObjectSerializer INSTANCE = new JavaObjectSerializer();

  @Override
  public byte[] serialize(final Object input) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutput out = new ObjectOutputStream(bos)) {
      out.writeObject(input);
      return bos.toByteArray();
    } catch (Exception e) {
      throw new EncodingFailedException(e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T deserialize(final Class<T> target, final byte[] input) {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(input);
         ObjectInput in = new ObjectInputStream(bis)) {
      return (T) in.readObject();
    } catch (Exception ex) {
      throw new DecodingFailedException(ex);
    }
  }

}

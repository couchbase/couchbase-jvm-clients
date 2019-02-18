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

import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.error.EncodingFailedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class SerializableContent<T extends Serializable> extends TypedContent<T> {

  static Serializable decode(final byte[] serialized) {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
         ObjectInput in = new ObjectInputStream(bis)) {
      return (Serializable) in.readObject();
    } catch (Exception ex) {
      throw new DecodingFailedException(ex);
    }
  }

  @SuppressWarnings({"unchecked"})
  public static <T extends Serializable> SerializableContent wrap(final T content) {
    return new SerializableContent(content);
  }

  private SerializableContent(T content) {
    super(content);
  }

  @Override
  public byte[] encoded() {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutput out = new ObjectOutputStream(bos)) {
      out.writeObject(content());
      return bos.toByteArray();
    } catch (Exception e) {
      throw new EncodingFailedException(e);
    }
  }

  @Override
  public int flags() {
    return Encoder.SERIALIZED_FLAGS;
  }
}

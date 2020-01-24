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
import org.junit.jupiter.api.Test;

import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SerializableTranscoderTest {

  @Test
  void shouldRoundtripSerializable() {
    User user = new User();
    user.firstname = "Michael";

    Transcoder.EncodedValue encoded = SerializableTranscoder.INSTANCE.encode(user);
    User decoded = SerializableTranscoder.INSTANCE.decode(User.class, encoded.encoded(), encoded.flags());
    assertEquals(decoded, user);
  }

  @Test
  void shouldFailIfNotSerializable() {
    InvalidArgumentException ex = assertThrows(
      InvalidArgumentException.class,
      () -> SerializableTranscoder.INSTANCE.encode(new Group())
    );
    assertTrue(ex.getCause() instanceof NotSerializableException);
  }

  static class User implements Serializable {

    public String firstname;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      User user = (User) o;
      return Objects.equals(firstname, user.firstname);
    }

    @Override
    public int hashCode() {
      return Objects.hash(firstname);
    }
  }

  static class Group {

  }

}
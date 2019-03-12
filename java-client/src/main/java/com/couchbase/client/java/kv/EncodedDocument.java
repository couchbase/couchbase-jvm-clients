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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;

import java.util.Arrays;

/**
 * The {@link EncodedDocument} has everything which is important for a document on the wire
 * and is needed for properly decoding it after reading it.
 *
 * <p>While the surface area is pretty small, we consider this advanced API and therefore it
 * is not marked as committed at this point.</p>
 *
 * @since 3.0.0
 */
@Stability.Volatile
public class EncodedDocument {

  private final int flags;
  private final byte[] content;

  public static EncodedDocument of(int flags, byte[] content) {
    return new EncodedDocument(flags, content);
  }

  private EncodedDocument(int flags, byte[] content) {
    this.flags = flags;
    this.content = content;
  }

  public int flags() {
    return flags;
  }

  public byte[] content() {
    return content;
  }

  @Override
  public String toString() {
    return "EncodedDocument{" +
      "flags=" + flags +
      ", content=" + new String(content, CharsetUtil.UTF_8) +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    EncodedDocument that = (EncodedDocument) o;

    if (flags != that.flags) return false;
    return Arrays.equals(content, that.content);
  }

  @Override
  public int hashCode() {
    int result = flags;
    result = 31 * result + Arrays.hashCode(content);
    return result;
  }
}

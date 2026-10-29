/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.core.kv;

import com.couchbase.client.core.util.Bytes;

/**
 * Encapsulates an ID returned by the server for each stream.
 * <p>
 * The returned UUID does not define any sub-structure to the 16-bytes and the entire 16-bytes are in
 * network byte order.
 */
public class CoreRangeScanId {

  private final byte[] bytes = new byte[16];

  /**
   * Takes the ID from the start of the array (copying it).
   *
   * @throws IndexOutOfBoundsException if the array is shorter than an ID.
   */
  public CoreRangeScanId(byte[] body) {
    System.arraycopy(body, 0, bytes, 0, bytes.length);
  }

  public byte[] bytes() {
    return bytes.clone();
  }

  @Override
  public String toString() {
    return "CoreRangeScanId{0x" + Bytes.toHex(bytes) + '}';
  }

}

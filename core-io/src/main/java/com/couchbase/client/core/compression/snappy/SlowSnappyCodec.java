/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.compression.snappy;

// CHECKSTYLE:OFF IllegalImport - Codecs are allowed to access Snappy library

import com.couchbase.client.core.compression.snappy.repackaged.org.iq80.snappy.v04.SlowSnappy;

/**
 * Backed by orq.io80.snappy version 0.4, modified to only use `SlowMemory`
 * so as not to be vulnerable to CVE-2024-36124.
 */
class SlowSnappyCodec implements SnappyCodec {
  @Override
  public byte[] compress(byte[] uncompressed) {
    return SlowSnappy.compress(uncompressed);
  }

  @Override
  public byte[] decompress(byte[] compressed, int offset, int len) {
    return SlowSnappy.uncompress(compressed, offset, len);
  }
}

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

package com.couchbase.client.core.protostellar.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.google.protobuf.ByteString;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class ProtostellarCoreEncodedContent {
  private final ByteString encoded;
  private final int flags;
  private final boolean compressed;
  private final long encodingTimeNanos;

  public ProtostellarCoreEncodedContent(ByteString encoded, int flags, boolean compressed, long encodingTimeNanos) {
    this.encoded = requireNonNull(encoded);
    this.flags = flags;
    this.compressed = compressed;
    this.encodingTimeNanos = encodingTimeNanos;
  }

  public ByteString bytes() {
    return encoded;
  }

  public int flags() {
    return flags;
  }

  public boolean compressed() {
    return compressed;
  }

  public long encodingTimeNanos() {
    return encodingTimeNanos;
  }
}

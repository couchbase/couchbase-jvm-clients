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

package com.couchbase.client.core.msg.query;

import com.couchbase.client.core.msg.chunk.ChunkHeader;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class QueryChunkHeader implements ChunkHeader {

  private final String requestId;
  private final Optional<byte[]> signature;
  private final Optional<String> clientContextId;

  public QueryChunkHeader(String requestId, Optional<String> clientContextId, Optional<byte[]> signature) {
    this.requestId = requestId;
    this.signature = signature;
    this.clientContextId = clientContextId;
  }

  public String requestId() {
    return requestId;
  }

  public Optional<byte[]> signature() {
    return signature;
  }

  public Optional<String> clientContextId() {
    return clientContextId;
  }

  @Override
  public String toString() {
    return "QueryChunkHeader{" +
      "requestId='" + requestId + '\'' +
      ", signature=" + signature.map(v -> new String(v, StandardCharsets.UTF_8)) +
      ", clientContextId=" + clientContextId +
      '}';
  }
}

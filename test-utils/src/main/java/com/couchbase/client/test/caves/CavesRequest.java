/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.test.caves;

// CHECKSTYLE:OFF IllegalImport - Allow unbundled Jackson

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class CavesRequest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final CompletableFuture<CavesResponse> response;

  private final Map<String, Object> payload;

  public CavesRequest(Map<String, Object> payload) {
    response = new CompletableFuture<>();
    this.payload = payload;
  }

  public CompletableFuture<CavesResponse> response() {
    return response;
  }

  public ByteBuf encode(final ChannelHandlerContext ctx) {
    try {
      byte[] encoded = MAPPER.writeValueAsBytes(payload);
      ByteBuf buf = ctx.alloc().buffer(encoded.length + 1);
      buf.writeBytes(encoded);
      buf.writeByte('\0');
      return buf;
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to encode caves request", e);
    }
  }

}

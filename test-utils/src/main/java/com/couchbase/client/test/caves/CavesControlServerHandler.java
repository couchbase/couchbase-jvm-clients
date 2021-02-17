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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class CavesControlServerHandler extends ChannelDuplexHandler {

  private final ObjectMapper mapper = new ObjectMapper();

  public final CompletableFuture<Void> receivedHello = new CompletableFuture<>();

  private ByteBuf buffer;

  private final Queue<CavesRequest> requests = new ArrayDeque<>(12);

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    buffer = ctx.alloc().buffer();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    buffer.release();
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof CavesRequest) {
      ByteBuf encoded = ((CavesRequest) msg).encode(ctx);
      ctx.write(encoded, promise);
      requests.add((CavesRequest) msg);
    } else {
      throw new IllegalStateException("Unsupported request type: " + msg);
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    buffer.writeBytes(((ByteBuf) msg));
  }

  public CompletableFuture<Void> receivedHello() {
    return receivedHello;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    try {
      if (buffer.readableBytes() == 0) {
        return;
      }

      Map<String, Object> fromCaves = mapper.readValue(
        buffer.toString(StandardCharsets.UTF_8),
        new TypeReference<Map<String, Object>>() {});
      buffer.clear();

      if (fromCaves.get("type").equals("hello")) {
        receivedHello.complete(null);
      } else {
        CavesRequest request = requests.remove();
        request.response().complete(new CavesResponse(fromCaves));
      }
    } catch (Exception ex) {
      // ex.printStackTrace();
      // keep going
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
  }
}

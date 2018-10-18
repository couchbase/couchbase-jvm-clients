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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.GetResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noOpaque;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.status;

/**
 * This handler is responsible for encoding KV requests and completing them once
 * a response arrives.
 *
 * @since 2.0.0
 */
public class KeyValueMessageHandler extends ChannelDuplexHandler {

  private Request request;

  private final CoreContext coreContext;

  public KeyValueMessageHandler(final CoreContext coreContext) {
    this.coreContext = coreContext;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof Request) {
      this.request = (Request) msg;
      ctx.write(encode(ctx, (Request) msg));
      System.err.println("wrote...");
    } else {
      // todo: terminate this channel and raise an event, this is not supposed to happen
    }
  }

  private ByteBuf encode(ChannelHandlerContext ctx, final Request request) {
    if (request instanceof GetRequest) {
      return encodeGet(ctx, (GetRequest) request);
    }
    return null;
  }

  private ByteBuf encodeGet(ChannelHandlerContext ctx, final GetRequest request) {
    ByteBuf key = Unpooled.wrappedBuffer(request.key());
    ByteBuf r = MemcacheProtocol.request(
      ctx.alloc(),
      MemcacheProtocol.Opcode.GET,
      noDatatype(),
      noPartition(),
      noOpaque(),
      noCas(),
      noExtras(),
      key,
      noBody());
    key.release();
    return r;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    System.err.println("Reading!" + msg);

    if (msg instanceof ByteBuf) {
      System.err.println("--->  0x" + Integer.toHexString(status((ByteBuf) msg)));
      request.succeed(new GetResponse());
    } else {
      // todo: ERROR!!
    }
  }

}

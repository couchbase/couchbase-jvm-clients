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

package com.couchbase.client.core.io.netty.query;

import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.util.yasjl.ByteBufJsonParser;
import com.couchbase.client.core.util.yasjl.Callbacks.JsonPointerCB1;
import com.couchbase.client.core.util.yasjl.JsonPointer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

import java.io.EOFException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This handler is responsible for writing Query requests and completing their associated responses
 * once they arrive.
 *
 * @since 2.0.0
 */
public class QueryMessageHandler extends ChannelDuplexHandler {

  private QueryRequest currentRequest;
  private QueryResponse currentResponse;
  private final AtomicLong toRead;
  private ByteBuf responseContent;

  private ByteBufJsonParser parser = new ByteBufJsonParser(new JsonPointer[] {
    new JsonPointer("/results/-", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        // TODO GP this looks expensive, could we instead just pass up data and ROW without creating an object?
        currentResponse.subscriber().onNext(new QueryResponse.QueryEvent(QueryResponse.QueryEventType.ROW, data));
      }
    }),

    new JsonPointer("/errors/-", new JsonPointerCB1() {
        @Override
        public void call(final ByteBuf value) {
            byte[] data = new byte[value.readableBytes()];
            value.readBytes(data);
            value.release();
            currentResponse.subscriber().onNext(new QueryResponse.QueryEvent(QueryResponse.QueryEventType.ERROR, data));
        }
    })
  });

  public QueryMessageHandler() {
    toRead = new AtomicLong(0);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    ctx.fireChannelActive();
    responseContent = ctx.alloc().buffer();
    parser.initialize(responseContent);
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof QueryRequest) {
      currentRequest = (QueryRequest) msg;
      FullHttpRequest encoded = ((QueryRequest) msg).encode();
      encoded.headers().set(HttpHeaderNames.HOST, "127.0.0.1");
      ctx.write(encoded);
    } else {
      // todo: terminate this channel and raise an event, this is not supposed to happen
    }
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if (msg instanceof HttpResponse) {
      currentResponse = new QueryResponse(ResponseStatus.SUCCESS, currentRequest.subscriber()) {
        @Override
        public void request(long rows) {
          toRead.set(rows + toRead.get());
          ctx.channel().config().setAutoRead(true);
        }

        @Override
        public void cancel() {
          // TODO:
          System.err.println("--> cancelled! if not done ensure proper cleanup");
        }
      };

      currentRequest.succeed(currentResponse);
      ctx.channel().config().setAutoRead(false);
    } else if (msg instanceof HttpContent) {
      boolean last = msg instanceof LastHttpContent;
      responseContent.writeBytes(((HttpContent) msg).content());

      try {
        parser.parse();
        //discard only if EOF is not thrown
        responseContent.discardReadBytes();
      } catch (EOFException e) {
        // ignore, we are waiting for more data.
      }

      if (last) {
        currentResponse.cancel();
        currentResponse.subscriber().onComplete();
        responseContent.clear();
      } else {
        if (toRead.decrementAndGet() <= 0) {
          ctx.channel().config().setAutoRead(false);
        }
      }
    } else {
      // todo: error since a type returned that was not expected
    }

    ReferenceCountUtil.release(msg);
  }

}

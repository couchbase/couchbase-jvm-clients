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
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.util.ResponseStatusConverter;
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
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import reactor.core.scheduler.Scheduler;
import java.io.EOFException;
import java.nio.charset.Charset;

/**
 * This handler is responsible for writing Query requests and completing their associated responses
 * once they arrive.
 *
 * @since 2.0.0
 */
public class QueryMessageHandler extends ChannelDuplexHandler {
  private QueryRequest currentRequest;
  private QueryResponse currentResponse;
  private ByteBuf responseContent;
  private final Scheduler scheduler;
  private ByteBufJsonParser parser;
  private static final Charset CHARSET = CharsetUtil.UTF_8;

  public QueryMessageHandler(Scheduler scheduler) {
    this.scheduler = scheduler;
    this.parser = new ByteBufJsonParser(new JsonPointer[]{
      new JsonPointer("/results/-", new JsonPointerCB1() {
        @Override
        public void call(final ByteBuf value) {
          byte[] data = new byte[value.readableBytes()];
            value.readBytes(data);
            value.release();
            currentResponse.rows().onNext(data);
            currentResponse.rowRequestCompleted();
        }
      }), new JsonPointer("/requestID/-", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        String requestID = value.toString(CHARSET);
        requestID = requestID.substring(1, requestID.length() - 1);
        value.release();
        currentResponse.requestId().onNext(requestID);
      }
    }), new JsonPointer("/errors/-", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        currentResponse.errors().onNext(data);
      }
    }), new JsonPointer("/warnings/-", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        currentResponse.warnings().onNext(data);
      }
    }), new JsonPointer("/clientContextID", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        String clientContextID = value.toString(CHARSET);
        clientContextID = clientContextID.substring(1, clientContextID.length() - 1);
        value.release();
        currentResponse.clientContextId().onNext(clientContextID);
      }
    }), new JsonPointer("/metrics", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        currentResponse.metrics().onNext(data);
      }
    }), new JsonPointer("/status", new JsonPointerCB1() {
      @Override
      public void call(final ByteBuf value) {
        String statusStr = value.toString(CHARSET);
        statusStr = statusStr.substring(1, statusStr.length() - 1);
        value.release();
        currentResponse.queryStatus().onNext(statusStr);
      }
    })
    });
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    ctx.fireChannelActive();

  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof QueryRequest) {
      currentRequest = (QueryRequest) msg;
      FullHttpRequest encoded = ((QueryRequest) msg).encode();
      encoded.headers().set(HttpHeaderNames.HOST, "127.0.0.1");
      ctx.write(encoded);
    } else {
    }
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if (msg instanceof HttpResponse) {
      responseContent = ctx.alloc().buffer();
      parser.initialize(responseContent);
      currentResponse = new QueryResponse(ResponseStatusConverter.fromHttp(((HttpResponse)msg).status().code()), ctx.channel());
      currentRequest.succeed(currentResponse);
    }
    if (msg instanceof HttpContent) {
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
        responseContent.clear();
        currentResponse.complete();
      }
    }
    ReferenceCountUtil.release(msg);
  }
}

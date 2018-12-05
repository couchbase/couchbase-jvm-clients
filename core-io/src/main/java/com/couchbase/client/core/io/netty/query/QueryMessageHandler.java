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
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

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

  public QueryMessageHandler() {
    toRead = new AtomicLong(0);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    ctx.fireChannelActive();
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof QueryRequest) {
      currentRequest = (QueryRequest) msg;
      ctx.write(((QueryRequest) msg).encode());
    } else {
      // todo: terminate this channel and raise an event, this is not supposed to happen
    }
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if (msg instanceof HttpResponse) {
      currentResponse = new QueryResponse(ResponseStatus.SUCCESS, currentRequest.consumer()) {
        @Override
        public void request(long rows) {
          toRead.set(rows + toRead.get());
          ctx.channel().config().setAutoRead(true);
        }
      };

      currentRequest.succeed(currentResponse);
      ctx.channel().config().setAutoRead(false);
    } else if (msg instanceof HttpContent) {
      HttpContent content = (HttpContent) msg;
      boolean last = msg instanceof LastHttpContent;

      // todo: this needs to be fed through the streaming parser !! ...
      byte[] chunk = new byte[content.content().readableBytes()];
      content.content().readBytes(chunk);
      currentResponse.consumer().accept(new QueryResponse.Row(QueryResponse.RowType.ROW, chunk));

      if (last) {
        currentResponse.consumer().accept(new QueryResponse.Row(QueryResponse.RowType.END, null));
      } else {
        if (toRead.decrementAndGet() <= 0) {
          ctx.channel().config().setAutoRead(false);
        }
      }
    }

    ReferenceCountUtil.release(msg);
  }

}

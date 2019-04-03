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

package com.couchbase.client.core.io.netty;

import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.io.netty.chunk.ChunkedMessageHandler;
import com.couchbase.client.core.msg.Request;

/**
 * Parent class for all pipelines which need to switch between a chunked and a non-chunked handler structure.
 *
 * <p>The basic idea is that this handler is added to the pipeline always in the endpoint and it will decide
 * based on the chunkedClass on construction when to add the chunked handler and when to add the non-chunked
 * counterpart. Note that it will only change the handler structure if the "wrong" one is already in place.</p>
 *
 * <p>This switcher will also always put in the chunked one by default since this is the most likely and perf
 * critical variant of the two.</p>
 *
 * @since 2.0.0
 */
public class ChunkedHandlerSwitcher extends ChannelDuplexHandler {

  public static final String SWITCHER_IDENTIFIER = ChunkedHandlerSwitcher.class.getSimpleName();
  private static final String CHUNKED_IDENTIFIER = ChunkedMessageHandler.class.getSimpleName();

  private final ChunkedMessageHandler chunkedHandler;
  private final NonChunkedHttpMessageHandler nonChunkedHandler;
  private final Class<? extends Request> chunkedClass;

  private boolean chunkedHandlerActive = false;

  /**
   * Creates a new chunked handler switcher.
   *
   * @param chunkedHandler the handler which does the chunking.
   * @param nonChunkedHandler the handler which handles all the other msgs.
   * @param chunkedClass the class of request representing the chunked request.
   */
  protected ChunkedHandlerSwitcher(final ChunkedMessageHandler chunkedHandler,
                                   final NonChunkedHttpMessageHandler nonChunkedHandler,
                                   final Class<? extends Request> chunkedClass) {
    this.chunkedHandler = chunkedHandler;
    this.nonChunkedHandler = nonChunkedHandler;
    this.chunkedClass = chunkedClass;
  }

  /**
   * When the channel becomes active, make sure that the chunked handler is added since that is the most
   * likely needed one upfront (and the most perf critical one).
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    activateChunkedHandler(ctx);
  }

  /**
   * When a request comes along, this code decides if and how to switch out the handlers.
   *
   * @param ctx the channel handler context.
   * @param msg the incoming message type that should be written.
   * @param promise the channel promise, will just be passed through.
   */
  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
    boolean isChunked = chunkedClass.isAssignableFrom(msg.getClass());

    if (isChunked && !chunkedHandlerActive) {
      deactivateNonChunkedHandler(ctx);
      activateChunkedHandler(ctx);
    } else if (!isChunked && chunkedHandlerActive) {
      deactivateChunkedHandler(ctx);
      activateNonChunkedHandler(ctx);
    }

    ctx.write(msg, promise);
  }

  /**
   * Activates the chunked handler by adding it to the pipeline.
   *
   * @param ctx the channel handler context.
   */
  private void activateChunkedHandler(final ChannelHandlerContext ctx) {
    ctx.pipeline().addBefore(SWITCHER_IDENTIFIER, CHUNKED_IDENTIFIER, chunkedHandler);
    chunkedHandler.channelActive(ctx);
    chunkedHandlerActive = true;
  }

  /**
   * Removes the chunked handler from the pipeline.
   *
   * @param ctx the channel handler context.
   */
  private void deactivateChunkedHandler(final ChannelHandlerContext ctx) {
    ctx.pipeline().remove(CHUNKED_IDENTIFIER);
    chunkedHandlerActive = false;
  }

  /**
   * Activates the non-chunk handler by adding it to the pipeline.
   *
   * @param ctx the channel handler context.
   * @throws Exception propagated from the nested channelActive call.
   */
  private void activateNonChunkedHandler(final ChannelHandlerContext ctx) throws Exception {
    ctx.pipeline().addBefore(SWITCHER_IDENTIFIER, NonChunkedHttpMessageHandler.IDENTIFIER, nonChunkedHandler);
    nonChunkedHandler.channelActive(ctx);
  }

  /**
   * Removes the non-chunk handler from the pipeline.
   *
   * @param ctx the channel handler context.
   */
  private void deactivateNonChunkedHandler(final ChannelHandlerContext ctx) {
    ctx.pipeline().remove(NonChunkedHttpMessageHandler.IDENTIFIER);
  }

}

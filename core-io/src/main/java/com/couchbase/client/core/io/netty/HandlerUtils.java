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

package com.couchbase.client.core.io.netty;

import com.couchbase.client.core.cnc.events.io.ChannelClosedProactivelyEvent;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.io.IoContext;

/**
 * Various netty IO handler utilities.
 */
public class HandlerUtils {

  private HandlerUtils() {
    // cannot be constructed
  }

  /**
   * Proactively close this channel with the given reason.
   *
   * @param ioContext the io context.
   * @param ctx the channel context.
   * @param reason the reason why the channel is closed.
   */
  public static void closeChannelWithReason(final IoContext ioContext, final ChannelHandlerContext ctx,
                                            final ChannelClosedProactivelyEvent.Reason reason) {
    ctx.channel().close().addListener(v ->
      ioContext.environment().eventBus().publish(new ChannelClosedProactivelyEvent(ioContext, reason))
    );
  }

}

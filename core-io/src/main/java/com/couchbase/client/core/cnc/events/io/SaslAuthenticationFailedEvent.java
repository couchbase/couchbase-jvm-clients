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

package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.io.IoContext;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.time.Duration;

/**
 * If something goes wrong during the SASL authentication process, this event is raised.
 *
 * <p>Usually when this event is raised, the channel in the context has been closed and
 * the connect process aborted, since without authentication no data can flow. This should
 * be treated as an important problem to be looked at and therefore has an ERROR severity.</p>
 *
 * <p>A little more context might be provided from the description.</p>
 *
 * @since 2.0.0
 */
public class SaslAuthenticationFailedEvent extends AbstractEvent {

  private final String description;
  private final byte[] lastPacket;

  public SaslAuthenticationFailedEvent(final Duration duration, final IoContext context,
                                       final String description, final byte[] lastPacket) {
    super(Severity.ERROR, Category.IO, duration, context);
    this.description = description;
    this.lastPacket = lastPacket;
  }

  @Override
  public String description() {
    if (lastPacket != null && lastPacket.length > 0) {
      return description + "; Last Packet: \n"
        + ByteBufUtil.prettyHexDump(Unpooled.copiedBuffer(lastPacket))
        + "\n";
    }
    return description;
  }

  public byte[] lastPacket() {
    return lastPacket;
  }
}

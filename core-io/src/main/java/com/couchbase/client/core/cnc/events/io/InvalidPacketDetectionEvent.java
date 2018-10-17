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
 * This event is created if somewhere in the IO layer an invalid packet
 * was detected.
 *
 * <p>Usually this happens if during a KV operation either a malformed
 * request or response was detected. For more information, refer to the
 * payload of this event.</p>
 *
 * @since 2.0.0
 */
public class InvalidPacketDetectionEvent extends AbstractEvent {

  /**
   * This is a byte array and not a {@link io.netty.buffer.ByteBuf} since it can come from
   * many places and also we don't want to carry a potentially pooled entity around.
   */
  private final byte[] packet;

  public InvalidPacketDetectionEvent(final IoContext context, final byte[] packet) {
    super(Severity.ERROR, Category.IO, Duration.ZERO, context);
    this.packet = packet;
  }

  @Override
  public String description() {
    return "Invalid Packet detected: \n"
      + ByteBufUtil.prettyHexDump(Unpooled.copiedBuffer(packet))
      + "\n";
  }

  /**
   * Returns the invalid/malformed packet in its entirety.
   *
   * @return the malformed packet.
   */
  public byte[] packet() {
    return packet;
  }
}

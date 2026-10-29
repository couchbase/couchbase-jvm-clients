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
import com.couchbase.client.core.io.TrafficCaptureFormat;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;

import java.time.Duration;
import java.util.Arrays;

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
public class InvalidPacketDetectedEvent extends AbstractEvent {

  private final byte[] packet;

  public InvalidPacketDetectedEvent(final IoContext context, final byte[] packet) {
    super(Severity.ERROR, Category.IO, Duration.ZERO, context);
    this.packet = withoutSaslSecrets(packet);
  }

  /**
   * Returns a SASL packet's header, without its body (which may hold credentials) or the body's length
   * (a PLAIN body's length gives away the password's). Returns any other packet as it is.
   */
  private static byte[] withoutSaslSecrets(final byte[] packet) {
    if (packet.length < 2) {
      return packet;
    }
    MemcacheProtocol.Magic magic = MemcacheProtocol.Magic.of(packet[0]);
    byte opcode = packet[1];
    boolean sasl = magic != null && !magic.isServerPush()
      && (opcode == MemcacheProtocol.Opcode.SASL_AUTH.opcode() || opcode == MemcacheProtocol.Opcode.SASL_STEP.opcode());
    if (!sasl) {
      return packet;
    }
    byte[] header = Arrays.copyOf(packet, Math.min(packet.length, MemcacheProtocol.HEADER_SIZE));
    Arrays.fill(header, Math.min(header.length, 8), Math.min(header.length, 12), (byte) 0); // total body length
    return header;
  }

  @Override
  public String description() {
    return "Invalid Packet detected: \n"
      + TrafficCaptureFormat.hexDump(packet)
      + "\n";
  }

  /**
   * Returns the invalid/malformed packet in its entirety, except for a SASL packet: then only its header,
   * without the body's length, since the body may hold credentials.
   *
   * @return the malformed packet.
   */
  public byte[] packet() {
    return packet;
  }
}

/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.service;

import com.couchbase.client.core.cnc.events.io.InvalidPacketDetectedEvent;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.io.repackaged.tlschannel.NeedsReadException;
import com.couchbase.client.core.io.repackaged.tlschannel.NeedsWriteException;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import static java.util.Objects.requireNonNull;

/**
 * Reads a KV connection's packets: splits what it reads into packets (see {@link KeyValueFrameDecoder}), and checks
 * each one.
 * <p>
 * Used by the connecting thread during the handshake, then by the connection's event loop, so anything read ahead
 * during the handshake isn't lost. Reads from a channel (non-blocking, if the channel is). Not thread-safe.
 */
@NullMarked
final class KeyValuePacketReader {
  private final ReadableByteChannel channel;
  private final IoContext ioContext;
  private final KeyValueFrameDecoder frames;

  /**
   * @param channel the channel to read (see {@link #fillAvailable}).
   * @param minRead the least room to make for each read (see {@link KeyValueFrameDecoder#KeyValueFrameDecoder(int)}).
   * @param ioContext for reporting invalid packets.
   */
  KeyValuePacketReader(ReadableByteChannel channel, int minRead, IoContext ioContext) {
    this.channel = requireNonNull(channel);
    this.ioContext = requireNonNull(ioContext);
    this.frames = new KeyValueFrameDecoder(minRead);
  }

  /**
   * Returns the next complete packet already read, or null if there isn't one.
   *
   * @throws InvalidPacketException if the packet failed verification.
   */
  @Nullable KeyValuePacket next() throws InvalidPacketException {
    KeyValuePacket packet;
    try {
      packet = frames.next();
    } catch (KeyValueFrameDecoder.InvalidSizeException e) {
      throw invalidPacket(e.header);
    }
    if (packet != null && !isResponseOrServerPush(packet)) {
      throw invalidPacket(packet.toByteArray());
    }
    return packet;
  }

  /**
   * Returns true if the packet is a response or a request the server pushes. (The frame decoder already made sure
   * the packet's size is the size its header gives.)
   */
  private static boolean isResponseOrServerPush(KeyValuePacket packet) {
    byte magic = packet.magic();
    return magic == MemcacheProtocol.Magic.RESPONSE.magic()
      || magic == MemcacheProtocol.Magic.FLEXIBLE_RESPONSE.magic()
      || magic == MemcacheProtocol.Magic.SERVER_PUSH_REQUEST.magic();
  }

  /**
   * Reads what the channel has. Doesn't wait if the channel is non-blocking.
   *
   * @return how many bytes were read (0 if the channel had none).
   * @throws EOFException if the server closed the connection.
   * @throws NeedsWriteException if the channel is a TLS channel that must write before it can read more
   * (for example, to answer a TLS 1.3 key update). Try again once the socket is writable.
   */
  int fillAvailable() throws IOException {
    int read;
    try {
      read = frames.fill(channel);
    } catch (NeedsReadException e) {
      return 0; // a TLS channel with nothing to read
    }
    if (read < 0) {
      throw new EOFException("The server closed the connection.");
    }
    return read;
  }

  /**
   * Returns true if the last {@link #fillAvailable} filled all the room it had, so the channel probably has more.
   */
  boolean lastFillFilledRoom() {
    return frames.lastFillFilledRoom();
  }

  private InvalidPacketException invalidPacket(byte[] packet) {
    ioContext.environment().eventBus().publish(new InvalidPacketDetectedEvent(ioContext, packet));
    return new InvalidPacketException();
  }

  /**
   * Thrown when a packet from the server fails verification.
   */
  static final class InvalidPacketException extends IOException {
    InvalidPacketException() {
      super("Invalid packet from server");
    }
  }
}

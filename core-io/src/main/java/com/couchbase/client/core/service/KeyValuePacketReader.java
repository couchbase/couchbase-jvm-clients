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
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

/**
 * Reads a KV connection's packets: splits what it reads into packets (see {@link KeyValueFrameDecoder}), and checks
 * each one, like the Netty implementation's MemcacheProtocolVerificationHandler.
 * <p>
 * Used by the connecting thread during the handshake, then by the connection's reader thread, so anything read ahead
 * during the handshake isn't lost. Not thread-safe.
 */
@NullMarked
final class KeyValuePacketReader {
  private final InputStream input;
  private final IoContext ioContext;
  private final KeyValueFrameDecoder frames = new KeyValueFrameDecoder();

  /**
   * @param input the socket's input stream. Unbuffered: the frame decoder does the buffering.
   * @param ioContext for reporting invalid packets.
   */
  KeyValuePacketReader(InputStream input, IoContext ioContext) {
    this.input = requireNonNull(input);
    this.ioContext = requireNonNull(ioContext);
  }

  /**
   * Returns the next packet, reading from the socket as needed. It shares the read buffer's bytes,
   * so it's only valid until the next read (see {@link KeyValueFrameDecoder}).
   *
   * @throws InvalidPacketException if the packet failed verification
   * (like the Netty implementation's MemcacheProtocolVerificationHandler, it's reported, and the connection closed).
   */
  KeyValuePacket read() throws IOException {
    KeyValuePacket packet;
    while ((packet = next()) == null) {
      fill();
    }
    return packet;
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
   * Like {@link MemcacheProtocol#verifyResponse}. (The frame decoder already made sure the packet's size
   * is the size its header gives.)
   */
  private static boolean isResponseOrServerPush(KeyValuePacket packet) {
    byte magic = packet.magic();
    return magic == MemcacheProtocol.Magic.RESPONSE.magic()
      || magic == MemcacheProtocol.Magic.FLEXIBLE_RESPONSE.magic()
      || magic == MemcacheProtocol.Magic.SERVER_PUSH_REQUEST.magic();
  }

  /**
   * Reads more from the socket, waiting until there's something to read.
   *
   * @throws EOFException if the server closed the connection.
   */
  void fill() throws IOException {
    if (!frames.fill(input)) {
      throw new EOFException("The server closed the connection.");
    }
  }

  /**
   * Returns true if part of a packet has been read (so the rest is presumably on its way).
   */
  boolean hasUnreadBytes() {
    return frames.hasUnreadBytes();
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

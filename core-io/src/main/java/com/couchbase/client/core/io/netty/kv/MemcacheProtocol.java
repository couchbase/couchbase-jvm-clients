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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.msg.ResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.iq80.snappy.Snappy;

import java.util.Optional;

/**
 * The {@link MemcacheProtocol} class holds static helpers that deal with the encoding
 * and decoding as well as access of the memcache binary protocol.
 *
 * @since 2.0.0
 */
public enum MemcacheProtocol {
  ;

  /**
   * Magic byte identifying a request.
   */
  static final byte MAGIC_REQUEST = (byte) 0x80;

  /**
   * Magic byte for a response with flexible framing extras.
   */
  static final byte MAGIC_FLEXIBLE = (byte) 0x18;

  /**
   * Magic byte for a response without flexible framing extras.
   */
  static final byte MAGIC_RESPONSE = (byte) 0x81;

  /**
   * The fixed header size.
   */
  static final int HEADER_SIZE = 24;

  /**
   * The offset of the magic byte.
   */
  static final int MAGIC_OFFSET = 0;

  /**
   * The offset for the opcode.
   */
  static final int OPCODE_OFFSET = 1;

  /**
   * The offset for the datatype.
   */
  static final int DATATYPE_OFFSET = 5;

  /**
   * The offset of the status field.
   */
  static final int STATUS_OFFSET = 6;

  /**
   * The offset of the total length field.
   */
  static final int TOTAL_LENGTH_OFFSET = 8;

  /**
   * The offset for the opaque field.
   */
  static final int OPAQUE_OFFSET = 12;

  /**
   * The offset for the CAS field.
   */
  static final int CAS_OFFSET = 16;

  /**
   * Create a memcached protocol request with all fields necessary.
   *
   * @param alloc
   * @param opcode
   * @param datatype
   * @param partition
   * @param opaque
   * @param cas
   * @param extras
   * @param key
   * @param body
   * @return the created request.
   */
  public static ByteBuf request(final ByteBufAllocator alloc, final Opcode opcode, final byte datatype,
                         final short partition, final int opaque, final long cas,
                         final ByteBuf extras, final ByteBuf key, final ByteBuf body) {
    int keySize = key.readableBytes();
    int extrasSize = extras.readableBytes();
    int totalBodySize = extrasSize + keySize + body.readableBytes();
    return alloc
      .buffer(HEADER_SIZE + totalBodySize)
      .writeByte(MAGIC_REQUEST)
      .writeByte(opcode.opcode())
      .writeShort(keySize)
      .writeByte(extrasSize)
      .writeByte(datatype)
      .writeShort(partition)
      .writeInt(totalBodySize)
      .writeInt(opaque)
      .writeLong(cas)
      .writeBytes(extras)
      .writeBytes(key)
      .writeBytes(body);
  }

  /**
   * Returns the status of that response.
   *
   * @param message the memcache message to extract from.
   * @return the status field.
   */
  public static short status(final ByteBuf message) {
    return message.getShort(STATUS_OFFSET);
  }

  /**
   * Helper method to check if the given response has a successful status.
   *
   * @param message the memcache message to extract from.
   * @return true if success.
   */
  public static boolean successful(final ByteBuf message) {
    return status(message) == Status.SUCCESS.status();
  }

  /**
   * Helper method to return the opcode for the given request or response.
   *
   * @param message the message to get the opcode from.
   * @return the opcode as a byte.
   */
  static byte opcode(final ByteBuf message) {
    return message.getByte(OPCODE_OFFSET);
  }

  /**
   * Helper method to return the datatype from a request or response.
   *
   * @param message the message to get the datatype from.
   * @return the datatype as a byte.
   */
  public static byte datatype(final ByteBuf message) {
    return message.getByte(DATATYPE_OFFSET);
  }

  /**
   * Helper method to return the opaque value for the given request or response.
   *
   * @param message the message to get the opaque from.
   * @return the opaque as an int.
   */
  static int opaque(final ByteBuf message) {
    return message.getInt(OPAQUE_OFFSET);
  }

  /**
   * Helper method to extract the cas from a message.
   *
   * @param message the message to extract the cas from.
   * @return the cas as a long.
   */
  public static long cas(final ByteBuf message) {
    return message.getLong(CAS_OFFSET);
  }

  /**
   * Returns the body of the message if available.
   *
   * @param message the message of the body or empty if none found.
   * @return an optional either containing the body of the message or none.
   */
  public static Optional<ByteBuf> body(final ByteBuf message) {
    if (message == null) {
      return Optional.empty();
    }
    boolean flexible = message.getByte(0) == MAGIC_FLEXIBLE;

    int totalBodyLength = message.getInt(TOTAL_LENGTH_OFFSET);
    int keyLength = flexible ? message.getByte(3) : message.getShort(2);
    int flexibleExtrasLength = flexible ? message.getByte(2) : 0;
    byte extrasLength = message.getByte(4);
    int bodyLength = totalBodyLength - keyLength - extrasLength - flexibleExtrasLength;

    if (bodyLength > 0) {
      return Optional.of(message.slice(
          MemcacheProtocol.HEADER_SIZE + flexibleExtrasLength + extrasLength + keyLength,
          bodyLength
      ));
    } else {
      return Optional.empty();
    }
  }

  public static Optional<ByteBuf> extras(final ByteBuf message) {
    boolean flexible = message.getByte(0) == MAGIC_FLEXIBLE;
    byte extrasLength = message.getByte(4);
    int flexibleExtrasLength = flexible ? message.getByte(2) : 0;

    if (extrasLength > 0) {
      return Optional.of(message.slice(
        MemcacheProtocol.HEADER_SIZE + flexibleExtrasLength,
        extrasLength
      ));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Performs simple sanity checking of a key/value request.
   *
   * It checks the magic byte and if the total readable bytes match
   * up with the total length of the packet.
   *
   * @param request the request to check.
   * @return true if verified, false otherwise.
   */
  static boolean verifyRequest(final ByteBuf request) {
    int readableBytes = request.readableBytes();
    if (readableBytes < MemcacheProtocol.HEADER_SIZE) {
      return false;
    }
    byte magic = request.getByte(MAGIC_OFFSET);
    int bodyPlusHeader = request.getInt(TOTAL_LENGTH_OFFSET) + MemcacheProtocol.HEADER_SIZE;
    return magic == MemcacheProtocol.MAGIC_REQUEST && readableBytes == bodyPlusHeader;
  }

  /**
   * Performs simple sanity checking of a key/value response.
   *
   * It checks the magic byte and if the total readable bytes match
   * up with the total length of the packet.
   *
   * @param response the response to check.
   * @return true if verified, false otherwise.
   */
  static boolean verifyResponse(final ByteBuf response) {
    int readableBytes = response.readableBytes();
    if (readableBytes < MemcacheProtocol.HEADER_SIZE) {
      return false;
    }
    byte magic = response.getByte(MAGIC_OFFSET);
    int bodyPlusHeader = response.getInt(TOTAL_LENGTH_OFFSET) + MemcacheProtocol.HEADER_SIZE;

    return
      (magic == MemcacheProtocol.MAGIC_FLEXIBLE || magic == MemcacheProtocol.MAGIC_RESPONSE)
      && readableBytes == bodyPlusHeader;
  }

  /**
   * Helper to express no key is used for this message.
   */
  public static ByteBuf noKey() {
    return Unpooled.EMPTY_BUFFER;
  }

  /**
   * Helper to express no extras are used for this message.
   */
  public static ByteBuf noExtras() {
    return Unpooled.EMPTY_BUFFER;
  }

  /**
   * Helper to express no body is used for this message.
   */
  public static ByteBuf noBody() {
    return Unpooled.EMPTY_BUFFER;
  }

  /**
   * Helper to express no datatype is used for this message.
   */
  public static byte noDatatype() {
    return 0;
  }

  /**
   * Helper to express no partition is used for this message.
   */
  public static short noPartition() {
    return 0;
  }

  /**
   * Helper to express no opaque is used for this message.
   */
  public static int noOpaque() {
    return 0;
  }

  /**
   * Helper to express no cas is used for this message.
   */
  public static long noCas() {
    return 0;
  }

  /**
   * Decodes and converts the status from a message.
   *
   * <p>This is a convenience method usually used in decoders.</p>
   *
   * @param message the message to extract from.
   * @return the decoded status.
   */
  public static ResponseStatus decodeStatus(ByteBuf message) {
    return decodeStatus(status(message));
  }

  /**
   * Converts the KeyValue protocol status into its generic format.
   *
   * @param status the protocol status.
   * @return the response status.
   */
  public static ResponseStatus decodeStatus(short status) {
    if (status == Status.SUCCESS.status) {
      return ResponseStatus.SUCCESS;
    } else if (status == Status.NOT_FOUND.status) {
      return ResponseStatus.NOT_FOUND;
    } else if (status == Status.NOT_SUPPORTED.status) {
      return ResponseStatus.UNSUPPORTED;
    } else if (status == Status.ACCESS_ERROR.status) {
      return ResponseStatus.NO_ACCESS;
    } else if (status == Status.OUT_OF_MEMORY.status) {
      return ResponseStatus.OUT_OF_MEMORY;
    } else if (status == Status.SERVER_BUSY.status) {
      return ResponseStatus.SERVER_BUSY;
    } else if (status == Status.TEMPORARY_FAILURE.status) {
      return ResponseStatus.TEMPORARY_FAILURE;
    } else if (status == Status.NOT_MY_VBUCKET.status) {
      return ResponseStatus.NOT_MY_VBUCKET;
    } else {
      return ResponseStatus.UNKNOWN;
    }
  }

  /**
   * Try to compress the input, but if it is below the min ratio then it will return null.
   *
   * @param input the input array.
   * @param minRatio the minimum ratio to accept and return the buffer.
   * @return a {@link ByteBuf} if compressed, or null if below the min ratio.
   */
  public static ByteBuf tryCompression(byte[] input, double minRatio) {
    byte[] compressed = Snappy.compress(input);
    if (((double) compressed.length / input.length) > minRatio) {
      return null;
    }
    return Unpooled.wrappedBuffer(compressed);
  }

  /**
   * Try to decompress the input if the datatype has the snappy flag enabled.
   *
   * <p>If datatype does not indicate snappy enabled, then the input is returned
   * as presented.</p>
   *
   * @param input the input byte array.
   * @param datatype the datatype for the response.
   * @return the byte array, either decoded or the input straight.
   */
  public static byte[] tryDecompression(byte[] input, byte datatype) {
    if ((datatype & Datatype.SNAPPY.datatype()) == Datatype.SNAPPY.datatype()) {
      return Snappy.uncompress(input, 0, input.length);
    }
    return input;
  }

  /**
   * Helper method during development and debugging to dump the raw message as a
   * verbose string.
   */
  public static String messageToString(final ByteBuf message) {
    StringBuilder sb = new StringBuilder();

    byte magic = message.getByte(MAGIC_OFFSET);
    sb.append(String.format("Magic: 0x%x (%s)\n", magic, Magic.of(magic)));
    sb.append(String.format("Opcode: 0x%x\n", opcode(message)));

    if (Magic.of(magic).isFlexible()) {
      sb.append(String.format("Framing Extras Length: %d\n", message.getByte(2)));
      sb.append(String.format("Key Length: %d\n", message.getByte(3)));
    } else {
      sb.append(String.format("Key Length: %d\n", message.getShort(2)));
    }

    sb.append(String.format("Extras Length: %d\n", message.getByte(4)));
    sb.append(String.format("Datatype: 0x%x\n", datatype(message)));

    if (Magic.of(magic).isRequest()) {
      sb.append(String.format("VBucket ID: 0x%x\n", status(message)));
    } else {
      sb.append(String.format("Status: 0x%x\n", status(message)));
    }

    sb.append(String.format("Total Body Length: %d\n", message.getByte(TOTAL_LENGTH_OFFSET)));
    sb.append(String.format("Opaque: 0x%x\n", opaque(message)));
    sb.append(String.format("CAS: 0x%x\n", cas(message)));

    return sb.toString();
  }

  public enum Magic {
    REQUEST((byte) 0x80),
    RESPONSE((byte) 0x81),
    FLEXIBLE_REQUEST((byte) 0x08),
    FLEXIBLE_RESPONSE((byte) 0x18);

    private final byte magic;

    Magic(byte magic) {
      this.magic = magic;
    }

    /**
     * Returns the magic for the given command.
     *
     * @return the magic for the command.
     */
    public byte magic() {
      return magic;
    }

    public static Magic of(byte input) {
      switch (input) {
        case (byte) 0x80:
          return Magic.REQUEST;
        case (byte) 0x81:
          return Magic.RESPONSE;
        case 0x08:
          return Magic.FLEXIBLE_REQUEST;
        case 0x18:
          return Magic.FLEXIBLE_RESPONSE;
      }
      return null;
    }

    public boolean isFlexible() {
      return this == FLEXIBLE_REQUEST || this == FLEXIBLE_RESPONSE;
    }

    public boolean isRequest() {
      return this == REQUEST || this == FLEXIBLE_REQUEST;
    }

  }

  /**
   * Contains all known/used kv protocol opcodes.
   */
  public enum Opcode {
    /**
     * The get command.
     */
    GET((byte) 0x00),
    /**
     * The set (upsert) command.
     */
    SET((byte) 0x01),
    /**
     * The add (insert) command.
     */
    ADD((byte) 0x02),
    /**
     * The replace command.
     */
    REPLACE((byte) 0x03),
    /**
     * The delete (remove) command.
     */
    DELETE((byte) 0x04),
    /**
     * The noop command.
     */
    NOOP((byte) 0x0a),
    /**
     * The hello command used during bootstrap to negoatiate the features.
     */
    HELLO((byte) 0x1f),
    /**
     * Command used to fetch the error map during the bootstrap process.
     */
    ERROR_MAP((byte) 0xfe),
    /**
     * Command used to select a specific bucket on a connection.
     */
    SELECT_BUCKET((byte) 0x89),
    /**
     * List all SASL auth mechanisms the server supports.
     */
    SASL_LIST_MECHS((byte) 0x20),
    /**
     * Initial auth step in the SASL negotiation.
     */
    SASL_AUTH((byte) 0x21),
    /**
     * Subsequent steps in the SASL negotiation.
     */
    SASL_STEP((byte) 0x22),
    /**
     * Returns the current configuration for the bucket ("cccp").
     */
    GET_CONFIG((byte) 0xb5),
    /**
     * Returns the ID of a collection/scope combination
     */
    COLLECTIONS_GET_CID((byte) 0xbb),
    /**
     * Subdocument lookup with more than one element.
     */
    SUBDOC_MULTI_LOOKUP((byte) 0xd0),
    /**
     * Allows to get a document and reset its expiry at the same time.
     */
    GET_AND_TOUCH((byte) 0x1d),
    /**
     * Allows to get a document and perform a write lock at the same time.
     */
    GET_AND_LOCK((byte) 0x94),
    /**
     * Performs an observe call with the CAS option.
     */
    OBSERVE_CAS((byte) 0x92),
    /**
     * A replica get operation.
     */
    GET_REPLICA((byte) 0x83);

    private final byte opcode;

    Opcode(byte opcode) {
      this.opcode = opcode;
    }

    /**
     * Returns the opcode for the given command.
     *
     * @return the opcode for the command.
     */
    public byte opcode() {
      return opcode;
    }
  }

  public enum Status {
    /**
     * Successful message.
     */
    SUCCESS((short) 0x00),
    /**
     * Entity not found.
     */
    NOT_FOUND((short) 0x01),
    /**
     * Not my vbucket.
     */
    NOT_MY_VBUCKET((short) 0x07),
    /**
     * Access problem.
     */
    ACCESS_ERROR((short) 0x24),
    /**
     * The server could temporarily not fulfill the requrst.
     */
    TEMPORARY_FAILURE((short) 0x86),
    /**
     * The server is busy for some reason.
     */
    SERVER_BUSY((short) 0x85),
    /**
     * The server is out of memory.
     */
    OUT_OF_MEMORY((short) 0x82),
    /**
     * Not supported.
     */
    NOT_SUPPORTED((short) 0x83);

    private final short status;

    Status(short status) {
      this.status = status;
    }

    /**
     * Returns the status code for the status enum.
     *
     * @return the status code.
     */
    public short status() {
      return status;
    }

  }

  public enum Datatype {
    /**
     * Snappy datatype used to signal compression.
     */
    SNAPPY((byte) 0x02);

    private final byte datatype;

    Datatype(byte datatype) {
      this.datatype = datatype;
    }

    /**
     * Returns the datatype byte for the given enum.
     *
     * @return the datatype.
     */
    public byte datatype() {
      return datatype;
    }
  }
}
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

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.cnc.events.io.DurabilityTimeoutCoercedEvent;
import com.couchbase.client.core.compression.snappy.SnappyCodec;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.error.context.SubDocumentErrorContext;
import com.couchbase.client.core.error.subdoc.DeltaInvalidException;
import com.couchbase.client.core.error.subdoc.DocumentNotJsonException;
import com.couchbase.client.core.error.subdoc.DocumentTooDeepException;
import com.couchbase.client.core.error.subdoc.NumberTooBigException;
import com.couchbase.client.core.error.subdoc.PathExistsException;
import com.couchbase.client.core.error.subdoc.PathInvalidException;
import com.couchbase.client.core.error.subdoc.PathMismatchException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.core.error.subdoc.PathTooDeepException;
import com.couchbase.client.core.error.subdoc.ValueInvalidException;
import com.couchbase.client.core.error.subdoc.ValueTooDeepException;
import com.couchbase.client.core.error.subdoc.XattrCannotModifyVirtualAttributeException;
import com.couchbase.client.core.error.subdoc.XattrInvalidKeyComboException;
import com.couchbase.client.core.error.subdoc.XattrNoAccessException;
import com.couchbase.client.core.error.subdoc.XattrUnknownMacroException;
import com.couchbase.client.core.error.subdoc.XattrUnknownVirtualAttributeException;
import com.couchbase.client.core.io.KeyValueRequestBuilder;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus;
import com.couchbase.client.core.util.IntMap;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.listCopyOf;
import static java.util.Collections.unmodifiableSet;

/**
 * The {@link MemcacheProtocol} class holds static helpers that deal with the encoding
 * and decoding as well as access of the memcache binary protocol.
 *
 * @since 2.0.0
 */
public enum MemcacheProtocol {
  ;

  private static final SnappyCodec snappy = SnappyCodec.instance();

  /**
   * Holds the max value a unsigned short can represent.
   */
  public static final int UNSIGNED_SHORT_MAX = 65535;

  /**
   * The fixed header size.
   */
  public static final int HEADER_SIZE = 24;

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
   * Flag which indicates that this flexible extra frame is for syc replication.
   */
  public static final byte SYNC_REPLICATION_FLEXIBLE_IDENT = 1 << 4;

  /**
   * Flag which indicates that this flexible extra frame is for preserve ttl.
   */
  public static final byte PRESERVE_TTL_FLEXIBLE_IDENT = 5 << 4;

  /**
   * Minimum sync durability timeout that can be set and which will override any lower
   * user-provided value.
   */
  public static final short SYNC_REPLICATION_TIMEOUT_FLOOR_MS = 1500;

  /**
   * The byte used to signal this is a tracing extras frame.
   */
  public static final byte FRAMING_EXTRAS_TRACING = 0x00;

  /**
   * Signals that read/write units have been returned.
   */
  public static final byte FRAMING_EXTRAS_READ_UNITS_USED = 0x01;
  public static final byte FRAMING_EXTRAS_WRITE_UNITS_USED = 0x02;

  /**
   * The server did not return units.
   */
  public static final int UNITS_NOT_PRESENT = -1;

  /**
   * Returns the raw (possibly compressed) content of the message,
   * or an empty Optional if the raw content length is zero.
   */
  // visible for testing
  public static class FlexibleExtras {
    // Will be <0 if not present in the server response
    public final int readUnits;
    public final int writeUnits;

    public final long serverDuration;

    public FlexibleExtras(int readUnits, int writeUnits, long serverDuration) {
      this.readUnits = readUnits;
      this.writeUnits = writeUnits;
      this.serverDuration = serverDuration;
    }

    public void injectExportableParams(final Map<String, Object> input) {
      if (readUnits != UNITS_NOT_PRESENT) {
        input.put("readUnits", readUnits);
      }
      if (writeUnits != UNITS_NOT_PRESENT) {
        input.put("writeUnits", writeUnits);
      }
      if (serverDuration != UNITS_NOT_PRESENT) {
        input.put("serverDuration", serverDuration);
      }
    }
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
   * Writes the flexible extras {@link #mutationFlexibleExtras} would return (if any) into the request's framing
   * extras, after checking the server supports them, like it.
   */
  public static void putMutationFlexibleExtras(
    KeyValueRequestBuilder builder,
    KeyValueRequest<?> request,
    Optional<DurabilityLevel> durabilityLevel,
    boolean preserveExpiry
  ) {
    KeyValueChannelContext ctx = builder.context();
    DurabilityLevel durability = durabilityLevel.orElse(DurabilityLevel.NONE);
    if (preserveExpiry && !ctx.preserveTtl()) {
      throw new FeatureNotAvailableException(
          "This version of Couchbase Server does not support preserving expiry when modifying documents.");
    }
    if (durability != DurabilityLevel.NONE && !ctx.syncReplicationEnabled()) {
      throw new DurabilityLevelNotAvailableException(KeyValueErrorContext.incompleteRequest(request));
    }

    if (preserveExpiry) {
      builder.framingExtras().putByte(PRESERVE_TTL_FLEXIBLE_IDENT);
    }
    if (durability != DurabilityLevel.NONE) {
      builder.framingExtras()
        .putByte(SYNC_REPLICATION_FLEXIBLE_IDENT | 0x03)
        .putByte(durability.code())
        .putShort(syncReplicationDeadline(request.timeout(), request.context()));
    }
  }

  /**
   * Sets the request's value: the content, compressed if compression is enabled, the content is big enough,
   * and it compresses well enough, in which case this also adds {@link Datatype#SNAPPY} to the datatype.
   */
  public static void putValueMaybeCompressed(
    KeyValueRequestBuilder builder,
    byte[] content
  ) {
    CompressionConfig config = builder.context().compressionConfig();
    if (config != null && config.enabled() && content.length >= config.minSize()) {
      byte[] compressed = new byte[snappy.maxCompressedLength(content.length)];
      int compressedLength = builder.compressor().compress(content, 0, content.length, compressed, 0);
      if (compressesWellEnough(compressedLength, content.length, config.minRatio())) {
        builder
          .value(compressed, compressedLength)
          .addDatatype(Datatype.SNAPPY.datatype());
        return;
      }
    }
    builder.value(content);
  }

  /**
   * The sync replication deadline (in milliseconds) to send for the given request timeout.
   * Publishes an event if it had to be coerced.
   */
  static int syncReplicationDeadline(Duration timeout, CoreContext ctx) {
    long userTimeout = timeout.toMillis();

    int deadline;
    if (userTimeout >= UNSIGNED_SHORT_MAX) {
      // -1 because 0xffff is going to be reserved by the cluster. 1ms less doesn't matter.
      deadline = UNSIGNED_SHORT_MAX - 1;
      ctx.environment().eventBus().publish(new DurabilityTimeoutCoercedEvent(ctx, userTimeout, deadline));
    } else {
      // per spec 90% of the timeout is used as the deadline
      deadline = (int) (userTimeout * 0.9);
    }

    if (deadline < SYNC_REPLICATION_TIMEOUT_FLOOR_MS) {
      deadline = SYNC_REPLICATION_TIMEOUT_FLOOR_MS;
      ctx.environment().eventBus().publish(new DurabilityTimeoutCoercedEvent(ctx, userTimeout, deadline));
    }
    return deadline;
  }

  /**
   * Converts the KeyValue protocol status into its generic format.
   * <p>
   * Note that only the most likely statuses are covered here, the rest is in {@link #decodeOtherStatus(short)} so
   * that the JIT can inline the method efficiently.
   *
   * @param status the protocol status.
   * @return the response status.
   */
  public static ResponseStatus decodeStatus(short status) {
    if (status == Status.SUCCESS.status) {
      return ResponseStatus.SUCCESS;
    } else if (status == Status.NOT_FOUND.status) {
      return ResponseStatus.NOT_FOUND;
    } else if (status == Status.EXISTS.status) {
      return ResponseStatus.EXISTS;
    }

    return decodeOtherStatus(status);
  }

  /**
   * Helper method to decode status codes which are not as likely on the hot code path.
   *
   * @param status the protocol status.
   * @return the response status.
   */
  private static ResponseStatus decodeOtherStatus(short status) {
     if (status == Status.NOT_SUPPORTED.status) {
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
    } else if (status == Status.LOCKED.status) {
      return ResponseStatus.LOCKED;
    } else if (status == Status.NOT_LOCKED.status) {
      return ResponseStatus.NOT_LOCKED;
    } else if (status == Status.TOO_BIG.status) {
      return ResponseStatus.TOO_BIG;
    } else if (status == Status.NOT_STORED.status) {
      return ResponseStatus.NOT_STORED;
    } else if (status == Status.DURABILITY_INVALID_LEVEL.status) {
      return ResponseStatus.DURABILITY_INVALID_LEVEL;
    } else if (status == Status.DURABILITY_IMPOSSIBLE.status) {
      return ResponseStatus.DURABILITY_IMPOSSIBLE;
    } else if (status == Status.SYNC_WRITE_AMBIGUOUS.status) {
      return ResponseStatus.SYNC_WRITE_AMBIGUOUS;
    } else if (status == Status.SYNC_WRITE_IN_PROGRESS.status) {
      return ResponseStatus.SYNC_WRITE_IN_PROGRESS;
    } else if (status == Status.SYNC_WRITE_RE_COMMIT_IN_PROGRESS.status) {
      return ResponseStatus.SYNC_WRITE_RE_COMMIT_IN_PROGRESS;
    } else if (status == Status.SUBDOC_MULTI_PATH_FAILURE.status
      || status == Status.SUBDOC_MULTI_PATH_FAILURE_DELETED.status
      || status == Status.SUBDOC_DOC_NOT_JSON.status
      || status == Status.SUBDOC_XATTR_INVALID_KEY_COMBO.status
      || status == Status.SUBDOC_DOC_TOO_DEEP.status
      || status == Status.SUBDOC_INVALID_COMBO.status
      || status == Status.SUBDOC_CAN_ONLY_REVIVE_DELETED_DOCUMENTS.status) {
      return ResponseStatus.SUBDOC_FAILURE;
    } else if (status == Status.SUBDOC_SUCCESS_DELETED_DOCUMENT.status) {
      return ResponseStatus.SUCCESS;
    } else if (status == Status.UNKNOWN_COLLECTION.status) {
      return ResponseStatus.UNKNOWN_COLLECTION;
    } else if (status == Status.NO_COLLECTIONS_MANIFEST.status) {
      return ResponseStatus.NO_COLLECTIONS_MANIFEST;
    } else if (status == Status.NO_BUCKET.status) {
      return ResponseStatus.NO_BUCKET;
    } else if (status == Status.INTERNAL_SERVER_ERROR.status) {
      return ResponseStatus.INTERNAL_SERVER_ERROR;
    } else if (status == Status.NOT_INITIALIZED.status) {
      return ResponseStatus.NOT_INITIALIZED;
    } else if (status == Status.INVALID_REQUEST.status) {
      return ResponseStatus.INVALID_REQUEST;
    } else  if (status == Status.CANNOT_APPLY_COLLECTIONS_MANIFEST.status) {
      return ResponseStatus.CANNOT_APPLY_COLLECTIONS_MANIFEST;
    } else  if (status == Status.COLLECTIONS_MANIFEST_AHEAD.status) {
      return ResponseStatus.COLLECTIONS_MANIFEST_AHEAD;
    } else  if (status == Status.UNKNOWN_SCOPE.status) {
      return ResponseStatus.UNKNOWN_SCOPE;
    } else if (status == Status.RATE_LIMITED_NETWORK_EGRESS.status
      || status == Status.RATE_LIMITED_NETWORK_INGRESS.status
      || status == Status.RATE_LIMITED_MAX_CONNECTIONS.status
      || status == Status.RATE_LIMITED_MAX_COMMANDS.status) {
      return ResponseStatus.RATE_LIMITED;
    } else if (status == Status.SCOPE_SIZE_LIMIT_EXCEEDED.status) {
      return ResponseStatus.QUOTA_LIMITED;
    } else if (status == Status.RANGE_SCAN_MORE.status) {
      return ResponseStatus.CONTINUE;
    } else if (status == Status.RANGE_SCAN_COMPLETE.status) {
      return ResponseStatus.COMPLETE;
    } else if (status == Status.RANGE_SCAN_CANCELLED.status) {
      return ResponseStatus.CANCELED;
    } else if (status == Status.RANGE_ERROR.status) {
       return ResponseStatus.RANGE_ERROR;
    } else if (status == Status.VBUUID_NOT_EQUAL.status) {
       return ResponseStatus.VBUUID_NOT_EQUAL;
    } else {
      return ResponseStatus.UNKNOWN;
    }
  }

  /**
   * Converts a KeyValue protocol status into its generic format.  It must be a status that can be returned from a
   * sub-document operation.
   *
   * @param status the protocol status.
   * @return the response status.
   */
  public static SubDocumentOpResponseStatus decodeSubDocumentStatus(short status) {
    if (status == Status.SUCCESS.status) {
      return SubDocumentOpResponseStatus.SUCCESS;
    } else if (status == Status.SUBDOC_PATH_NOT_FOUND.status) {
      return SubDocumentOpResponseStatus.PATH_NOT_FOUND;
    } else if (status == Status.SUBDOC_PATH_MISMATCH.status) {
      return SubDocumentOpResponseStatus.PATH_MISMATCH;
    } else if (status == Status.SUBDOC_PATH_INVALID.status) {
      return SubDocumentOpResponseStatus.PATH_INVALID;
    } else if (status == Status.SUBDOC_PATH_TOO_BIG.status) {
      return SubDocumentOpResponseStatus.PATH_TOO_BIG;
    } else if (status == Status.SUBDOC_DOC_TOO_DEEP.status) {
      return SubDocumentOpResponseStatus.DOC_TOO_DEEP;
    } else if (status == Status.SUBDOC_VALUE_CANTINSERT.status) {
      return SubDocumentOpResponseStatus.VALUE_CANTINSERT;
    } else if (status == Status.SUBDOC_DOC_NOT_JSON.status) {
      return SubDocumentOpResponseStatus.DOC_NOT_JSON;
    } else if (status == Status.SUBDOC_NUM_RANGE.status) {
      return SubDocumentOpResponseStatus.NUM_RANGE;
    } else if (status == Status.SUBDOC_DELTA_RANGE.status) {
      return SubDocumentOpResponseStatus.DELTA_RANGE;
    } else if (status == Status.SUBDOC_PATH_EXISTS.status) {
      return SubDocumentOpResponseStatus.PATH_EXISTS;
    } else if (status == Status.SUBDOC_VALUE_TOO_DEEP.status) {
      return SubDocumentOpResponseStatus.VALUE_TOO_DEEP;
    } else if (status == Status.SUBDOC_INVALID_COMBO.status) {
      return SubDocumentOpResponseStatus.INVALID_COMBO;
    } else if (status == Status.SUBDOC_MULTI_PATH_FAILURE.status) {
      return SubDocumentOpResponseStatus.MULTI_PATH_FAILURE;
    } else if (status == Status.SUBDOC_MULTI_PATH_FAILURE_DELETED.status) {
      return SubDocumentOpResponseStatus.MULTI_PATH_FAILURE;
    } else if (status == Status.SUBDOC_XATTR_INVALID_FLAG_COMBO.status) {
      return SubDocumentOpResponseStatus.XATTR_INVALID_FLAG_COMBO;
    } else if (status == Status.SUBDOC_XATTR_INVALID_KEY_COMBO.status) {
      return SubDocumentOpResponseStatus.XATTR_INVALID_KEY_COMBO;
    } else if (status == Status.SUBDOC_XATTR_UNKNOWN_MACRO.status) {
      return SubDocumentOpResponseStatus.XATTR_UNKNOWN_MACRO;
    } else if (status == Status.SUBDOC_SUCCESS_DELETED_DOCUMENT.status) {
      return SubDocumentOpResponseStatus.SUCCESS_DELETED_DOCUMENT;
    } else if (status == Status.SUBDOC_XATTR_UNKNOWN_VATTR.status) {
      return SubDocumentOpResponseStatus.XATTR_UNKNOWN_VATTR;
    } else if (status == Status.SUBDOC_XATTR_CANNOT_MODIFY_VATTR.status) {
      return SubDocumentOpResponseStatus.XATTR_CANNOT_MODIFY_VATTR;
    } else if (status == Status.SUBDOC_INVALID_XATTR_ORDER.status) {
      return SubDocumentOpResponseStatus.XATTR_INVALID_ORDER;
    } else if (status == Status.ACCESS_ERROR.status) {
      return SubDocumentOpResponseStatus.XATTR_NO_ACCESS;
    } else {
      return SubDocumentOpResponseStatus.UNKNOWN;
    }
  }

  /**
   * For any response that can be returned by a SubDocument command - path, document, or execution-based - map it to
   * an appropriate SubDocumentException.
   */
  public static CouchbaseException mapSubDocumentError(KeyValueRequest<?> request, SubDocumentOpResponseStatus status,
                                                       String path, int index, @Nullable MemcacheProtocol.FlexibleExtras flexibleExtras) {
    SubDocumentErrorContext ctx = new SubDocumentErrorContext(
      KeyValueErrorContext.completedRequest(request, ResponseStatus.SUBDOC_FAILURE, flexibleExtras),
      index,
      path,
      status,
      null,
      null
    );

    switch(status) {
      case PATH_NOT_FOUND:
        return new PathNotFoundException(ctx);
      case PATH_MISMATCH:
        return new PathMismatchException(ctx);
      case PATH_TOO_BIG:
        return new PathTooDeepException(ctx);
      case PATH_INVALID:
        return new PathInvalidException(ctx);
      case DOC_TOO_DEEP:
        return new DocumentTooDeepException(ctx);
      case VALUE_CANTINSERT:
        return new ValueInvalidException(ctx);
      case DOC_NOT_JSON:
        return new DocumentNotJsonException(ctx);
      case NUM_RANGE:
        return new NumberTooBigException(ctx);
      case DELTA_RANGE:
        return new DeltaInvalidException(ctx);
      case PATH_EXISTS:
        return new PathExistsException(ctx);
      case VALUE_TOO_DEEP:
        return new ValueTooDeepException(ctx);
      case XATTR_UNKNOWN_MACRO:
        return new XattrUnknownMacroException(ctx);
      case XATTR_INVALID_KEY_COMBO:
        return new XattrInvalidKeyComboException(ctx);
      case XATTR_UNKNOWN_VATTR:
        return new XattrUnknownVirtualAttributeException(ctx);
      case XATTR_CANNOT_MODIFY_VATTR:
        return new XattrCannotModifyVirtualAttributeException(ctx);
      case XATTR_NO_ACCESS:
        return new XattrNoAccessException(ctx);
      default:
        return new CouchbaseException("Unexpected SubDocument response code", ctx);
    }
  }

  /**
   * Try to compress the input, but if it is below the min ratio then it will return null.
   *
   * @param input the input array.
   * @param minRatio the minimum ratio to accept and return the compressed bytes.
   * @return the compressed bytes, or null if below the min ratio.
   */
  public static @Nullable byte[] tryCompression(byte[] input, double minRatio) {
    byte[] compressed = snappy.compress(input);
    return compressesWellEnough(compressed.length, input.length, minRatio) ? compressed : null;
  }

  /**
   * Returns true if the compressed length is at most the given ratio of the original length.
   * An empty value (ratio NaN) counts as compressing well enough.
   */
  private static boolean compressesWellEnough(int compressedLength, int originalLength, double minRatio) {
    return !(((double) compressedLength / originalLength) > minRatio);
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
    if (Datatype.isSnappy(datatype)) {
      return snappy.decompress(input);
    }
    return input;
  }

  public enum Magic {
    REQUEST(0x80),
    RESPONSE(0x81),
    FLEXIBLE_REQUEST(0x08),
    FLEXIBLE_RESPONSE(0x18),
    SERVER_PUSH_REQUEST(0x82),
    SERVER_PUSH_RESPONSE(0x83),
    ;

    private static final IntMap<Magic> codeToValue = IntMap.from(Magic.class, it -> it.magic() & 0xff);

    private final byte magic;

    Magic(int magic) {
      this.magic = (byte) require8Bit(magic);
    }

    /**
     * Returns the magic for the given command.
     *
     * @return the magic for the command.
     */
    public byte magic() {
      return magic;
    }

    public boolean isServerPush() {
      return this == SERVER_PUSH_REQUEST || this == SERVER_PUSH_RESPONSE;
    }

    @Nullable
    public static Magic of(byte input) {
      return codeToValue.get(input);
    }

    public boolean isFlexible() {
      return this == FLEXIBLE_REQUEST || this == FLEXIBLE_RESPONSE;
    }

    public boolean isRequest() {
      return this == REQUEST || this == FLEXIBLE_REQUEST;
    }

  }

  public enum ServerPushOpcode {
    CLUSTERMAP_CHANGE_NOTIFICATION(0x01),
    AUTHENTICATE(0x02),
    ACTIVE_EXTERNAL_USERS(0x03),
    ;

    private static final IntMap<ServerPushOpcode> codeToValue = IntMap.from(ServerPushOpcode.class, it -> it.opcode() & 0xff);

    private final byte opcode;

    ServerPushOpcode(int opcode) {
      this.opcode = (byte) require8Bit(opcode);
    }

    /**
     * Returns the opcode for the given command.
     *
     * @return the opcode for the command.
     */
    public byte opcode() {
      return opcode;
    }

    @Nullable
    public static ServerPushOpcode of(final byte input) {
      return codeToValue.get(input);
    }
  }

  /**
   * Contains all known/used kv protocol opcodes.
   */
  public enum Opcode {
    /**
     * The get command.
     */
    GET(0x00),
    /**
     * The set (upsert) command.
     */
    SET(0x01),
    /**
     * The add (insert) command.
     */
    ADD(0x02),
    /**
     * The replace command.
     */
    REPLACE(0x03),
    /**
     * The delete (remove) command.
     */
    DELETE(0x04),
    /**
     * Increment binary counter.
     */
    INCREMENT(0x05),
    /**
     * Decrement binary counter.
     */
    DECREMENT(0x06),
    /**
     * The noop command.
     */
    NOOP(0x0a),
    /**
     * Binary append.
     */
    APPEND(0x0e),
    /**
     * Binary prepend.
     */
    PREPEND(0x0f),
    /**
     * The hello command used during bootstrap to negoatiate the features.
     */
    HELLO(0x1f),
    /**
     * Command used to fetch the error map during the bootstrap process.
     */
    ERROR_MAP(0xfe),
    /**
     * Command used to select a specific bucket on a connection.
     */
    SELECT_BUCKET(0x89),
    /**
     * List all SASL auth mechanisms the server supports.
     */
    SASL_LIST_MECHS(0x20),
    /**
     * Initial auth step in the SASL negotiation.
     */
    SASL_AUTH(0x21),
    /**
     * Subsequent steps in the SASL negotiation.
     */
    SASL_STEP(0x22),
    /**
     * Returns the current configuration for the bucket ("cccp").
     */
    GET_CONFIG(0xb5),
    /**
     * Returns the ID of a collection/scope combination
     */
    COLLECTIONS_GET_CID(0xbb),
    /**
     * Subdocument lookup with more than one element.
     */
    SUBDOC_MULTI_LOOKUP(0xd0),
    /**
     * Subdocument multi mutation.
     */
    SUBDOC_MULTI_MUTATE(0xd1),
    /**
     * Allows to get a document and reset its expiry at the same time.
     */
    GET_AND_TOUCH(0x1d),
    /**
     * Allows to get a document and perform a write lock at the same time.
     */
    GET_AND_LOCK(0x94),
    /**
     * Performs an observe call with the CAS option.
     */
    OBSERVE_CAS(0x92),
    /**
     * Performs an observe call via sequence numbers.
     */
    OBSERVE_SEQ(0x91),
    /**
     * A replica get operation.
     */
    GET_REPLICA(0x83),
    /**
     * Touch command sets a new expiration.
     */
    TOUCH(0x1c),
    /**
     * Unlocks a write locked document.
     */
    UNLOCK(0x95),
    /**
     * Deletes (tombstones) a document while setting metadata.
     */
    DELETE_WITH_META(0xa8),
    /**
     * Returns the collections manifest for a bucket.
     */
    COLLECTIONS_GET_MANIFEST(0xba),

    /**
     * Fetches metadata for a document
     */
    GET_META(0xa0),
    /**
     * Create a new range scan.
     */
    @SinceCouchbase("7.2")
    RANGE_SCAN_CREATE(0xda),
    /**
     * Get more results from a range scan.
     */
    @SinceCouchbase("7.2")
    RANGE_SCAN_CONTINUE(0xdb),
    /**
     * Cancel a range scan. (Not required if scan completes normally.)
     */
    @SinceCouchbase("7.2")
    RANGE_SCAN_CANCEL(0xdc),
    ;

    private static final IntMap<Opcode> codeToValue = IntMap.from(Opcode.class, it -> it.opcode() & 0xff);

    private final byte opcode;

    Opcode(int opcode) {
      this.opcode = (byte) require8Bit(opcode);
    }

    /**
     * Returns the opcode for the given command.
     *
     * @return the opcode for the command.
     */
    public byte opcode() {
      return opcode;
    }

    @Nullable
    public static Opcode of(final byte input) {
      return codeToValue.get(input);
    }
  }

  public enum Status {
    /**
     * Successful message.
     */
    SUCCESS(0x00),
    /**
     * Entity not found.
     */
    NOT_FOUND(0x01),
    /**
     * The key exists in the cluster (with another CAS value).
     */
    EXISTS(0x02),
    /**
     * Resource too big.
     */
    TOO_BIG(0x03),
    /**
     * Invalid request sent.
     */
    INVALID_REQUEST(0x04),
    /**
     * Not stored for some reason.
     */
    NOT_STORED(0x05),
    /**
     * Not my vbucket.
     */
    NOT_MY_VBUCKET(0x07),
    /**
     * No bucket selected.
     */
    NO_BUCKET(0x08),
    /**
     * Resource is locked.
     */
    LOCKED(0x09),
    /**
     * Authentication error.
     */
    AUTH_ERROR(0x20),
    /**
     * When sampling, this error indicates the collection does not have enough keys to satisfy the requested sample size.
     */
    RANGE_ERROR(0x22),
    /**
     * Access problem.
     */
    ACCESS_ERROR(0x24),
    /**
     * The server/kv engine is not initialized yet.
     */
    NOT_INITIALIZED(0x25),
    /**
     * A server-internal error has been reported.
     */
    INTERNAL_SERVER_ERROR(0x84),
    /**
     * The server could temporarily not fulfill the requrst.
     */
    TEMPORARY_FAILURE(0x86),
    /**
     * The server is busy for some reason.
     */
    SERVER_BUSY(0x85),
    /**
     * Unknown command.
     */
    UNKNOWN_COMMAND(0x81),
    /**
     * The server is out of memory.
     */
    OUT_OF_MEMORY(0x82),
    /**
     * Not supported.
     */
    NOT_SUPPORTED(0x83),
    /**
     * The provided path does not exist in the document
     */
    SUBDOC_PATH_NOT_FOUND(0xc0),
    /**
     * One of path components treats a non-dictionary as a dictionary, or a non-array as an array, or value the path points to is not a number
     */
    SUBDOC_PATH_MISMATCH(0xc1),
    /**
     * The path's syntax was incorrect
     */
    SUBDOC_PATH_INVALID(0xc2),
    /**
     * The path provided is too large: either the string is too long, or it contains too many components
     */
    SUBDOC_PATH_TOO_BIG(0xc3),
    /**
     * The document has too many levels to parse
     */
    SUBDOC_DOC_TOO_DEEP(0xc4),
    /**
     * The value provided will invalidate the JSON if inserted
     */
    SUBDOC_VALUE_CANTINSERT(0xc5),
    /**
     * The existing document is not valid JSON
     */
    SUBDOC_DOC_NOT_JSON(0xc6),
    /**
     * The existing number is out of the valid range for arithmetic operations
     */
    SUBDOC_NUM_RANGE(0xc7),
    /**
     * The operation would result in a number outside the valid range
     */
    SUBDOC_DELTA_RANGE(0xc8),
    /**
     * The requested operation requires the path to not already exist, but it exists
     */
    SUBDOC_PATH_EXISTS(0xc9),
    /**
     * Inserting the value would cause the document to be too deep
     */
    SUBDOC_VALUE_TOO_DEEP(0xca),
    /**
     * An invalid combination of commands was specified
     */
    SUBDOC_INVALID_COMBO(0xcb),
    /**
     * Specified key was successfully found, but one or more path operations failed
     */
    SUBDOC_MULTI_PATH_FAILURE(0xcc),
    /**
     * An invalid combination of operationSpecified key was successfully found, but one or more path operations faileds, using macros when not using extended attributes
     */
    SUBDOC_XATTR_INVALID_FLAG_COMBO(0xce),
    /**
     * Only single xattr key may be accessed at the same time
     */
    SUBDOC_XATTR_INVALID_KEY_COMBO(0xcf),
    /**
     * The server has no knowledge of the requested macro
     */
    SUBDOC_XATTR_UNKNOWN_MACRO(0xd0),
    /**
     * Unknown virtual attribute.
     */
    SUBDOC_XATTR_UNKNOWN_VATTR(0xd1),
    /**
     * Cannot modify virtual attribute.
     */
    SUBDOC_XATTR_CANNOT_MODIFY_VATTR(0xd2),
    /**
     * The subdoc operation completed successfully on the deleted document
     */
    SUBDOC_SUCCESS_DELETED_DOCUMENT(0xcd),
    /**
     * The subdoc operation found the deleted document, but one or more path operations failed.
     */
    SUBDOC_MULTI_PATH_FAILURE_DELETED(0xd3),
    /**
     * Invalid ordering of the extended attributes.
     */
    SUBDOC_INVALID_XATTR_ORDER(0xd4),
    /**
     * ReviveDocument flag has been used on a document that's already alive.
     */
    SUBDOC_CAN_ONLY_REVIVE_DELETED_DOCUMENTS(0xd6),
    /**
     * Invalid request. Returned if an invalid durability level is specified.
     */
    DURABILITY_INVALID_LEVEL(0xa0),

    /**
     * Valid request, but given durability requirements are impossible to achieve.
     *
     * <p>because insufficient configured replicas are connected. Assuming level=majority and
     * C=number of configured nodes, durability becomes impossible if floor((C + 1) / 2)
     * nodes or greater are offline.</p>
     */
    DURABILITY_IMPOSSIBLE(0xa1),

    /**
     * Returned if an attempt is made to mutate a key which already has a SyncWrite pending.
     *
     * <p>Transient, the client would typically retry (possibly with backoff). Similar to
     * ELOCKED.</p>
     */
    SYNC_WRITE_IN_PROGRESS(0xa2),

    /**
     * Returned if the requested key has a SyncWrite which is being re-committed.
     *
     * <p>Transient, the client would typically retry (possibly with backoff). Similar to
     * ELOCKED.</p>
     */
    SYNC_WRITE_RE_COMMIT_IN_PROGRESS(0xa4),

    /**
     * The SyncWrite request has not completed in the specified time and has ambiguous result.
     *
     * <p>it may Succeed or Fail; but the final value is not yet known.</p>
     */
    SYNC_WRITE_AMBIGUOUS(0xa3),

    /**
     * The scan was cancelled whilst returning data. This could be the only status
     * if the cancel was noticed before a key/value was loaded.
     */
    RANGE_SCAN_CANCELLED(0xa5),
    /**
     * Scan has not reached the end key; more data maybe available.
     * Client should issue another range scan continue request.
     */
    RANGE_SCAN_MORE(0xa6),
    /**
     * Scan has reached the end of the range.
     */
    RANGE_SCAN_COMPLETE(0xa7),

    /**
     * The vbuuid as part of the snapshot requirements does not align with the server.
     */
    VBUUID_NOT_EQUAL(0xa8),

    /**
     * The collection ID provided is unknown, maybe it changed or got dropped.
     */
    UNKNOWN_COLLECTION(0x88),

    /**
     * No collections manifest has been set on the server.
     */
    NO_COLLECTIONS_MANIFEST(0x89),

    /**
     * Bucket Manifest update could not be applied to vbucket(s).
     */
    CANNOT_APPLY_COLLECTIONS_MANIFEST(0x8a),

    /**
     * We have a collection's manifest which is from the future. This means
     * they we have a uid that is greater than the servers.
     */
    COLLECTIONS_MANIFEST_AHEAD(0x8b),

    /**
     * Operation attempted with an unknown scope.
     */
    UNKNOWN_SCOPE(0x8c),

    /**
     * Rate limited: Network Ingress.
     */
    RATE_LIMITED_NETWORK_INGRESS(0x30),

    /**
     * Rate limited: Network Egress.
     */
    RATE_LIMITED_NETWORK_EGRESS(0x31),

    /**
     * Rate limited: Max Connections.
     */
    RATE_LIMITED_MAX_CONNECTIONS(0x32),

    /**
     * Rate limited: Max Commands.
     */
    RATE_LIMITED_MAX_COMMANDS(0x33),

    /**
     * The scope contains too much data.
     */
    SCOPE_SIZE_LIMIT_EXCEEDED(0x34),

    /**
     * Resource is not locked.
     * <p>
     * Added in 7.6.0 under MB-58088.
     */
    @SinceCouchbase("7.6")
    NOT_LOCKED(0x0e),
    ;

    private static final IntMap<Status> codeToValue = IntMap.from(Status.class, it -> it.status & 0xffff);

    private final short status;

    Status(int status) {
      this.status = (short) require16Bit(status);
    }

    /**
     * Returns the status code for the status enum.
     *
     * @return the status code.
     */
    public short status() {
      return status;
    }

    @Nullable
    public static Status of(short input) {
      return codeToValue.get(input);
    }
  }

  public enum Datatype {

    /**
     * The JSON datatype.
     */
    JSON(0x01),

    /**
     * Snappy datatype used to signal compression.
     */
    SNAPPY(0x02),

    /**
     * Extended attributes (XATTR)
     */
    XATTR(0x04),
    ;

    private final byte bitmask;

    Datatype(int bitmask) {
      this.bitmask = (byte) require8Bit(bitmask);
    }

    /**
     * Returns the datatype byte for the given enum.
     *
     * @return the datatype.
     */
    public byte datatype() {
      return bitmask;
    }

    public static Set<Datatype> decode(int bitfield) {
      // ANDing with allBits ignores any unrecognized bits,
      // and ensures the index is in bounds.
      return precomputed.get(bitfield & allBits);
    }

    public static int encode(Set<Datatype> type) {
      int bitfield = 0;
      for (Datatype it : type) {
        bitfield |= it.bitmask;
      }
      return bitfield;
    }

    private static final List<Datatype> valueList = listCopyOf(Arrays.asList(values()));

    // Precompute the values where at least one recognized bit is set.
    private static final int allBits = encode(EnumSet.allOf(Datatype.class));
    private static final List<Set<Datatype>> precomputed = new ArrayList<>(allBits + 1);

    static {
      for (int i = 0; i <= allBits; i++) {
        precomputed.add(unmodifiableSet(doDecode(i)));
      }
    }

    private static Set<Datatype> doDecode(int bitfield) {
      Set<Datatype> result = EnumSet.noneOf(Datatype.class);
      for (Datatype type : valueList) {
        if (contains(bitfield, type)) {
          result.add(type);
        }
      }
      return result;
    }

    public static boolean isSnappy(int bitfield) {
      return contains(bitfield, SNAPPY);
    }

    public static boolean contains(int bitfield, Datatype type) {
      return (bitfield & type.bitmask) != 0;
    }
  }

  private static int require8Bit(int value) {
    if ((value & 0xff) != value) {
      throw new IllegalArgumentException("Expected a value that fits in 8 bits, but got: 0x" + Integer.toHexString(value));
    }
    return value;
  }

  private static int require16Bit(int value) {
    if ((value & 0xffff) != value) {
      throw new IllegalArgumentException("Expected a value that fits in 16 bits, but got: 0x" + Integer.toHexString(value));
    }
    return value;
  }
}

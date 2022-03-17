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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.config.BucketCapabilities;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.CompositeByteBuf;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.error.context.SubDocumentErrorContext;
import com.couchbase.client.core.error.subdoc.DocumentAlreadyAliveException;
import com.couchbase.client.core.error.subdoc.DocumentNotJsonException;
import com.couchbase.client.core.error.subdoc.DocumentTooDeepException;
import com.couchbase.client.core.error.subdoc.XattrInvalidKeyComboException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.util.Bytes;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Status;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.cas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeSubDocumentStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.extractToken;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.flexibleRequest;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.mapSubDocumentError;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.mutationFlexibleExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.status;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SubdocMutateRequest extends BaseKeyValueRequest<SubdocMutateResponse> implements SyncDurabilityRequest {

  private static final byte SUBDOC_FLAG_XATTR_PATH = (byte) 0x04;
  private static final byte SUBDOC_FLAG_CREATE_PATH = (byte) 0x01;
  private static final byte SUBDOC_FLAG_EXPAND_MACRO = (byte) 0x10;

  private static final byte SUBDOC_DOC_FLAG_MKDOC = (byte) 0x01;
  private static final byte SUBDOC_DOC_FLAG_ADD = (byte) 0x02;
  public static final byte SUBDOC_DOC_FLAG_ACCESS_DELETED = (byte) 0x04;
  public static final byte SUBDOC_DOC_FLAG_CREATE_AS_DELETED = (byte) 0x08;
  private static final byte SUBDOC_DOC_FLAG_REVIVE = (byte) 0x10;

  public static final int SUBDOC_MAX_FIELDS = 16;

  private final byte flags;
  private final long expiration;
  private final boolean preserveExpiry;
  private final long cas;
  private final List<Command> commands;
  private final String origKey;
  private final Optional<DurabilityLevel> syncReplicationType;
  private final boolean createAsDeleted;
  private final boolean insertDocument;

  public SubdocMutateRequest(final Duration timeout, final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                             final BucketConfig bucketConfig, final RetryStrategy retryStrategy, final String key,
                             final boolean insertDocument, final boolean upsertDocument, final boolean reviveDocument,
                             final boolean accessDeleted, final boolean createAsDeleted,
                             final List<Command> commands, long expiration,
                             boolean preserveExpiry,
                             long cas,
                             final Optional<DurabilityLevel> syncReplicationType, final RequestSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    this.insertDocument = insertDocument;
    byte flags = 0;

    if (createAsDeleted) {
      if (!bucketConfig.bucketCapabilities().contains(BucketCapabilities.CREATE_AS_DELETED)) {
        throw new FeatureNotAvailableException("Cannot use createAsDeleted Sub-Document flag, as it is not supported by this version of the cluster");
      }
    }

    if (reviveDocument) {
      if (!bucketConfig.bucketCapabilities().contains(BucketCapabilities.SUBDOC_REVIVE_DOCUMENT)) {
          throw new FeatureNotAvailableException("Cannot use ReviveDocument Sub-Document flag, as it is not supported by this version of the cluster");
      }
    }

    if (insertDocument && upsertDocument) {
      throw InvalidArgumentException.fromMessage("Cannot both insert and upsert full document");
    }

    if (cas != 0 && (insertDocument || upsertDocument)) {
      throw InvalidArgumentException.fromMessage("A cas value can only be applied to \"replace\" store semantics.");
    }

    if (preserveExpiry) {
      if (insertDocument) {
        throw InvalidArgumentException.fromMessage("When using 'insert' store semantics, must not specify `preserveExpiry`.");
      }
      if (!upsertDocument && expiration != 0) {
        throw InvalidArgumentException.fromMessage("When using 'replace' store semantics (the default), must not specify both `expiry` and `preserveExpiry`.");
      }
    }

    if (upsertDocument) {
      flags |= SUBDOC_DOC_FLAG_MKDOC;
    }

    if (insertDocument) {
      flags |= SUBDOC_DOC_FLAG_ADD;
    }

    if (reviveDocument) {
      flags |= SUBDOC_DOC_FLAG_REVIVE;
    }

    if (accessDeleted) {
      flags |= SUBDOC_DOC_FLAG_ACCESS_DELETED;
    }

    if (createAsDeleted) {
      flags |= SUBDOC_DOC_FLAG_CREATE_AS_DELETED;
    }

    this.flags = flags;
    this.commands = commands;
    this.expiration = expiration;
    this.preserveExpiry = preserveExpiry;
    this.cas = cas;
    this.origKey = key;
    this.syncReplicationType = syncReplicationType;
    this.createAsDeleted = createAsDeleted;

    if (span != null) {
      span.attribute(TracingIdentifiers.ATTR_OPERATION, TracingIdentifiers.SPAN_REQUEST_KV_MUTATE_IN);
      applyLevelOnSpan(syncReplicationType, span);
    }
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    ByteBuf key = null;
    ByteBuf extras = null;
    ByteBuf content = null;
    ByteBuf flexibleExtras = mutationFlexibleExtras(this, ctx, alloc, syncReplicationType, preserveExpiry);

    try {
      if (createAsDeleted && !ctx.createAsDeleted()) {
        // Memcached 6.5.0 and below will reset the connection if this flag is sent, hence checking the createAsDeleted HELO
        // This should never trigger, it should be preempted by the BucketCapabilities.CREATE_AS_DELETED check above.
        // It is left purely as an additional safety measure.
        throw new FeatureNotAvailableException("Cannot use createAsDeleted Sub-Document flag, as it is not supported by this version of the cluster");
      }

      key = encodedKeyWithCollection(alloc, ctx);

      extras = alloc.buffer();
      if (expiration != 0) {
        extras.writeInt((int) expiration);
      }
      if (flags != 0) {
        extras.writeByte(flags);
      }

      if (commands.size() == 1) {
        content = commands.get(0).encode(alloc);
      } else {
        content = alloc.compositeBuffer(commands.size());
        for (Command command : commands) {
          ByteBuf commandBuffer = command.encode(alloc);
          try {
            ((CompositeByteBuf) content).addComponent(commandBuffer);
            content.writerIndex(content.writerIndex() + commandBuffer.readableBytes());
          } catch (Exception ex) {
            ReferenceCountUtil.release(commandBuffer);
            throw ex;
          }
        }
      }

      return flexibleRequest(alloc, Opcode.SUBDOC_MULTI_MUTATE, noDatatype(), partition(), opaque,
          cas, flexibleExtras, extras, key, content);

    } finally {
      ReferenceCountUtil.release(key);
      ReferenceCountUtil.release(extras);
      ReferenceCountUtil.release(flexibleExtras);
      ReferenceCountUtil.release(content);
    }
  }

  @Override
  public SubdocMutateResponse decode(final ByteBuf response, KeyValueChannelContext ctx) {
    Optional<ByteBuf> maybeBody = body(response);
    short rawOverallStatus = status(response);
    ResponseStatus overallStatus = decodeStatus(response);
    Optional<CouchbaseException> error = Optional.empty();

    SubDocumentField[] values = null;

    if (maybeBody.isPresent()) {
      ByteBuf body = maybeBody.get();

      // If there's a multi-mutation failure we only get the first failure back
      if (rawOverallStatus == Status.SUBDOC_MULTI_PATH_FAILURE.status()
        || rawOverallStatus == Status.SUBDOC_MULTI_PATH_FAILURE_DELETED.status()) {
        byte index = body.readByte();
        short opStatusRaw = body.readShort();
        SubDocumentOpResponseStatus opStatus = decodeSubDocumentStatus(opStatusRaw);
        Command c = commands.get(index);
        error = Optional.of(mapSubDocumentError(this, opStatus, c.path, c.originalIndex));
        values = new SubDocumentField[0];
      } else if (overallStatus.success()) {
        // "For successful multi mutations, there will be zero or more results; each of the results containing a value."
        values = new SubDocumentField[commands.size()];

        // Check we can read index (1 byte) and status (2 bytes), else we're done
        int INDEX_PLUS_STATUS_FIELDS_BYTES = 3;
        while (body.isReadable(INDEX_PLUS_STATUS_FIELDS_BYTES)) {
          byte index = body.readByte();
          Command command = commands.get(index);

          // "Status of the mutation. If the status indicates success, the next two fields are applicable. If it is an
          // error then the result has been fully read"
          short statusRaw = body.readShort();
          SubDocumentOpResponseStatus status = decodeSubDocumentStatus(statusRaw);

          if (status != SubDocumentOpResponseStatus.SUCCESS) {
            CouchbaseException err = mapSubDocumentError(this, status, command.path, command.originalIndex);

            SubDocumentField op = new SubDocumentField(status, Optional.of(err), Bytes.EMPTY_BYTE_ARRAY, command.path, command.type);
            values[command.originalIndex] = op;
          } else {
            int valueLength = body.readInt();
            byte[] value = new byte[valueLength];
            body.readBytes(value, 0, valueLength);
            SubDocumentField op = new SubDocumentField(status, Optional.empty(), value, command.path, command.type);
            values[command.originalIndex] = op;
          }
        }
      }
    }

    if (values == null) {
      values = new SubDocumentField[0];
    }

    // Handle any document-level failures here
    if (rawOverallStatus == Status.SUBDOC_DOC_NOT_JSON.status()) {
      SubDocumentErrorContext e = createSubDocumentExceptionContext(SubDocumentOpResponseStatus.DOC_NOT_JSON);
      error = Optional.of(new DocumentNotJsonException(e));
    } else if (rawOverallStatus == Status.SUBDOC_DOC_TOO_DEEP.status()) {
      SubDocumentErrorContext e = createSubDocumentExceptionContext(SubDocumentOpResponseStatus.DOC_TOO_DEEP);
      error = Optional.of(new DocumentTooDeepException(e));
    } else if (rawOverallStatus == Status.SUBDOC_XATTR_INVALID_KEY_COMBO.status()) {
      SubDocumentErrorContext e = createSubDocumentExceptionContext(SubDocumentOpResponseStatus.XATTR_INVALID_KEY_COMBO);
      error = Optional.of(new XattrInvalidKeyComboException(e));
    } else if (rawOverallStatus == Status.SUBDOC_CAN_ONLY_REVIVE_DELETED_DOCUMENTS.status()) {
      SubDocumentErrorContext e = createSubDocumentExceptionContext(SubDocumentOpResponseStatus.CAN_ONLY_REVIVE_DELETED_DOCUMENTS);
      error = Optional.of(new DocumentAlreadyAliveException(e));
    }
    // Note that error is only ultimately thrown if response.status() == SUBDOC_FAILURE

    // Do not handle SUBDOC_INVALID_COMBO here, it indicates a client-side bug
    return new SubdocMutateResponse(
      overallStatus,
      error,
      values,
      cas(response),
      extractToken(ctx.mutationTokensEnabled(), partition(), response, ctx.bucket().get())
    );
  }

  private SubDocumentErrorContext createSubDocumentExceptionContext(SubDocumentOpResponseStatus status) {
    return new SubDocumentErrorContext(
            KeyValueErrorContext.completedRequest(this, ResponseStatus.SUBDOC_FAILURE),
            0,
            null,
            status
    );
  }

  public static InvalidArgumentException errIfNoCommands(ErrorContext errorContext) {
    return new InvalidArgumentException(
      "Argument validation failed",
      InvalidArgumentException.fromMessage("No SubDocument commands provided"),
      errorContext
    );
  }

  public static InvalidArgumentException errIfTooManyCommands(ErrorContext errorContext) {
    return new InvalidArgumentException(
      "Argument validation failed",
      InvalidArgumentException.fromMessage("A maximum of " + SubdocMutateRequest.SUBDOC_MAX_FIELDS + " fields can be provided"),
      errorContext
    );
  }

  public boolean insertDocument() {
    return insertDocument;
  }

  public static class Command {
    private final static byte[] EMPTY_ARRAY = new byte[] {};

    private final SubdocCommandType type;
    private final String path;
    @Nullable private final byte[] fragment;
    private final boolean createParent;
    private final boolean xattr;
    private final boolean expandMacro;
    private final int originalIndex;

    public Command(SubdocCommandType type, String path, @Nullable byte[] fragment,
                   boolean createParent, boolean xattr, boolean expandMacro, int originalIndex) {
      this.type = type;
      this.path = path;
      this.xattr = xattr;
      this.fragment = fragment == null ? EMPTY_ARRAY : fragment;
      this.createParent = createParent;
      this.expandMacro = expandMacro;
      this.originalIndex = originalIndex;
    }

    public ByteBuf encode(final ByteBufAllocator alloc) {
      byte[] path = this.path.getBytes(UTF_8);
      int pathLength = path.length;

      ByteBuf buffer = alloc.buffer(8 + pathLength + fragment.length);
      buffer.writeByte(type.opcode());
      byte flags = 0;
      if (xattr) {
        flags |= SUBDOC_FLAG_XATTR_PATH;
      }
      if(createParent) {
        flags |= SUBDOC_FLAG_CREATE_PATH;
      }
      if(expandMacro) {
        flags |= SUBDOC_FLAG_EXPAND_MACRO;
      }
      buffer.writeByte(flags);
      buffer.writeShort(pathLength);
      buffer.writeInt(fragment.length);
      buffer.writeBytes(path);
      buffer.writeBytes(fragment);

      return buffer;
    }

    public int originalIndex() {
      return originalIndex;
    }

    public boolean xattr() {
      return xattr;
    }
  }

  @Override
  public Optional<DurabilityLevel> durabilityLevel() {
    return syncReplicationType;
  }

  @Override
  public String name() {
    return "mutate_in";
  }
}

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
import com.couchbase.client.core.cnc.InternalSpan;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException;
import com.couchbase.client.core.error.ErrorContext;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.KeyValueErrorContext;
import com.couchbase.client.core.error.subdoc.DocumentNotJsonException;
import com.couchbase.client.core.error.subdoc.DocumentTooDeepException;
import com.couchbase.client.core.error.subdoc.SubDocumentErrorContext;
import com.couchbase.client.core.error.subdoc.XattrInvalidKeyComboException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.CompositeByteBuf;
import com.couchbase.client.core.util.Bytes;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SubdocMutateRequest extends BaseKeyValueRequest<SubdocMutateResponse> implements SyncDurabilityRequest {

  public static final String OPERATION_NAME = "subdoc_mutate";

  private static final byte SUBDOC_FLAG_XATTR_PATH = (byte) 0x04;
  private static final byte SUBDOC_FLAG_CREATE_PATH = (byte) 0x01;
  private static final byte SUBDOC_FLAG_EXPAND_MACRO = (byte) 0x10;

  private static final byte SUBDOC_DOC_FLAG_MKDOC = (byte) 0x01;
  private static final byte SUBDOC_DOC_FLAG_ADD = (byte) 0x02;
  public static final byte SUBDOC_DOC_FLAG_ACCESS_DELETED = (byte) 0x04;

  public static final int SUBDOC_MAX_FIELDS = 16;

  private final byte flags;
  private final long expiration;
  private final long cas;
  private final List<Command> commands;
  private final String origKey;
  private final Optional<DurabilityLevel> syncReplicationType;


  public SubdocMutateRequest(final Duration timeout, final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                             final RetryStrategy retryStrategy, final String key,
                             final boolean insertDocument, final boolean upsertDocument, final boolean accessDeleted,
                             final List<Command> commands, long expiration, long cas,
                             final Optional<DurabilityLevel> syncReplicationType, final InternalSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    byte flags = 0;

    if (insertDocument && upsertDocument) {
      throw InvalidArgumentException.fromMessage("Cannot both insert and upsert full document");
    }

    if (upsertDocument) {
      flags |= SUBDOC_DOC_FLAG_MKDOC;
    }

    if (insertDocument) {
      flags |= SUBDOC_DOC_FLAG_ADD;
    }

    if (accessDeleted) {
      flags |= SUBDOC_DOC_FLAG_ACCESS_DELETED;
    }

    this.flags = flags;
    this.commands = commands;
    this.expiration = expiration;
    this.cas = cas;
    this.origKey = key;
    this.syncReplicationType = syncReplicationType;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    ByteBuf key = null;
    ByteBuf extras = null;
    ByteBuf content = null;
    ByteBuf flexibleExtras = null;

    try {
      key = encodedKeyWithCollection(alloc, ctx);

      extras = alloc.buffer();
      if (flags != 0) {
        extras.writeByte(flags);
      }
      if (expiration != 0) {
        extras.writeInt((int) expiration);
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

      ByteBuf request;
      if (syncReplicationType.isPresent()) {
        if (ctx.syncReplicationEnabled()) {
          flexibleExtras = flexibleSyncReplication(alloc, syncReplicationType.get(), timeout(), context());
          request = flexibleRequest(alloc, Opcode.SUBDOC_MULTI_MUTATE, noDatatype(), partition(), opaque,
            cas, flexibleExtras, extras, key, content);
        }
        else {
          throw new DurabilityLevelNotAvailableException(KeyValueErrorContext.incompleteRequest(this));
        }
      } else {
        request = request(alloc, Opcode.SUBDOC_MULTI_MUTATE, noDatatype(), partition(), opaque,
          cas, extras, key, content);
      }
      return request;
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

    SubDocumentField[] values;

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
      } else {
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
    } else {
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
    }

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

  public static class Command {

    private final SubdocCommandType type;
    private final String path;
    private final byte[] fragment;
    private final boolean createParent;
    private final boolean xattr;
    private final boolean expandMacro;
    private final int originalIndex;

    public Command(SubdocCommandType type, String path, byte[] fragment,
                   boolean createParent, boolean xattr, boolean expandMacro, int originalIndex) {
      this.type = type;
      this.path = path;
      this.xattr = xattr;
      this.fragment = fragment;
      this.createParent = createParent;
      this.expandMacro = expandMacro;
      this.originalIndex = originalIndex;
    }

    public ByteBuf encode(final ByteBufAllocator alloc) {
      byte[] path = this.path.getBytes(UTF_8);
      int pathLength = path.length;

      ByteBuf buffer = alloc.buffer(4 + pathLength + fragment.length);
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

}

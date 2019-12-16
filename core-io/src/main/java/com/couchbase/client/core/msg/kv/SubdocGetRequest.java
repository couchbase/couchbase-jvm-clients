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
import com.couchbase.client.core.error.KeyValueErrorContext;
import com.couchbase.client.core.error.subdoc.DocumentNotJsonException;
import com.couchbase.client.core.error.subdoc.DocumentTooDeepException;
import com.couchbase.client.core.error.subdoc.SubDocumentErrorContext;
import com.couchbase.client.core.error.subdoc.XattrInvalidKeyComboException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.CompositeByteBuf;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SubdocGetRequest extends BaseKeyValueRequest<SubdocGetResponse> {

  public static final String OPERATION_NAME = "subdoc_get";

  private static final byte SUBDOC_FLAG_XATTR_PATH = (byte) 0x04;
  private static final byte SUBDOC_FLAG_ACCESS_DELETED = (byte) 0x08;

  private final byte flags;
  private final List<Command> commands;
  private final String origKey;

  public SubdocGetRequest(final Duration timeout, final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                          final RetryStrategy retryStrategy, final String key,
                          final byte flags, final List<Command> commands, final InternalSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    this.flags = flags;
    this.commands = commands;
    this.origKey = key;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, ChannelContext ctx) {
    ByteBuf key = null;
    ByteBuf extras = null;
    ByteBuf body = null;

    try {
      key = encodedKeyWithCollection(alloc, ctx);

      if (flags != 0) {
        extras = alloc.buffer(Byte.BYTES).writeByte(flags);
      }

      if (commands.size() == 1) {
        // Note currently the only subdoc error response handled is ERR_SUBDOC_MULTI_PATH_FAILURE.  Make sure to
        // add the others if do the single lookup optimisation.
        // Update: single subdoc optimization will not be supported.  It adds just 3 bytes to the package size and gives
        // minimal performance gains, in return for additional client complexity.
        body = commands.get(0).encode(alloc);
      } else {
        body = alloc.compositeBuffer(commands.size());
        for (Command command : commands) {
          ByteBuf commandBuffer = command.encode(alloc);
          try {
            ((CompositeByteBuf) body).addComponent(commandBuffer);
            body.writerIndex(body.writerIndex() + commandBuffer.readableBytes());
          } catch (Exception ex) {
            ReferenceCountUtil.release(commandBuffer);
            throw ex;
          }
        }
      }

      return request(
        alloc,
        MemcacheProtocol.Opcode.SUBDOC_MULTI_LOOKUP,
        noDatatype(),
        partition(),
        opaque,
        noCas(),
        extras == null ? noExtras() : extras,
        key,
        body
      );
    } finally {
      ReferenceCountUtil.release(key);
      ReferenceCountUtil.release(body);
      ReferenceCountUtil.release(extras);
    }
  }

  @Override
  public SubdocGetResponse decode(final ByteBuf response, ChannelContext ctx) {
    Optional<ByteBuf> maybeBody = body(response);
    SubDocumentField[] values;
    List<CouchbaseException> errors = null;
    if (maybeBody.isPresent()) {
      ByteBuf body = maybeBody.get();
      values = new SubDocumentField[commands.size()];
      for (Command command : commands) {
        short statusRaw = body.readShort();
        SubDocumentOpResponseStatus status = decodeSubDocumentStatus(statusRaw);
        Optional<CouchbaseException> error = Optional.empty();
        if (status != SubDocumentOpResponseStatus.SUCCESS) {
          if (errors == null) errors = new ArrayList<>();
          CouchbaseException err = mapSubDocumentError(this, status, command.path, command.originalIndex());
          errors.add(err);
          error = Optional.of(err);
        }
        int valueLength = body.readInt();
        byte[] value = new byte[valueLength];
        body.readBytes(value, 0, valueLength);
        SubDocumentField op = new SubDocumentField(status, error, value, command.path, command.type);
        values[command.originalIndex] = op;
      }
    } else {
      values = new SubDocumentField[0];
    }

    short rawStatus = status(response);
    ResponseStatus status = decodeStatus(response);

    Optional<CouchbaseException> error = Optional.empty();

    // Note that we send all subdoc requests as multi currently so always get this back on error
    if (rawStatus == Status.SUBDOC_MULTI_PATH_FAILURE.status()
        || rawStatus == Status.SUBDOC_MULTI_PATH_FAILURE_DELETED.status()) {
      // Special case logic for CMD_EXISTS
      if (commands.size() == 1 && commands.get(0).type == SubdocCommandType.EXISTS) {
        status = ResponseStatus.SUCCESS;
      }
      // If a single subdoc op was tried and failed, retursn that directly
      else if (commands.size() == 1 && errors != null && errors.size() == 1) {
        error = Optional.of(errors.get(0));
      }
      else {
        // Otherwise return success, as some of the operations have succeeded
        status = ResponseStatus.SUCCESS;
      }
    }

    // Handle any document-level failures here
    if (rawStatus == Status.SUBDOC_DOC_NOT_JSON.status()) {
      SubDocumentErrorContext e = createSubDocumentExceptionContext(SubDocumentOpResponseStatus.DOC_NOT_JSON);
      error = Optional.of(new DocumentNotJsonException(e));
    } else if (rawStatus == Status.SUBDOC_DOC_TOO_DEEP.status()) {
      SubDocumentErrorContext e = createSubDocumentExceptionContext(SubDocumentOpResponseStatus.DOC_TOO_DEEP);
      error = Optional.of(new DocumentTooDeepException(e));
    } else if (rawStatus == Status.SUBDOC_XATTR_INVALID_KEY_COMBO.status()) {
      SubDocumentErrorContext e = createSubDocumentExceptionContext(SubDocumentOpResponseStatus.XATTR_INVALID_KEY_COMBO);
      error = Optional.of(new XattrInvalidKeyComboException(e));
    }

    // Do not handle SUBDOC_INVALID_COMBO here, it indicates a client-side bug
    return new SubdocGetResponse(status, error, values, cas(response));
  }

  private SubDocumentErrorContext createSubDocumentExceptionContext(SubDocumentOpResponseStatus status) {
    return new SubDocumentErrorContext(
      KeyValueErrorContext.completedRequest(this, ResponseStatus.SUBDOC_FAILURE),
      0,
      null,
      status
    );
  }

  public static class Command {
    private final SubdocCommandType type;
    private final String path;
    private final boolean xattr;
    private final int originalIndex;

    public Command(SubdocCommandType type, String path, boolean xattr, int originalIndex) {
      this.type = type;
      this.path = path;
      this.xattr = xattr;
      this.originalIndex = originalIndex;
    }

    public ByteBuf encode(ByteBufAllocator alloc) {
      byte[] path = this.path.getBytes(UTF_8);
      int pathLength = path.length;

      ByteBuf buffer = alloc.buffer(4 + pathLength);
      buffer.writeByte(type.opcode());
      if (xattr) {
        buffer.writeByte(SUBDOC_FLAG_XATTR_PATH);
      } else {
        buffer.writeByte(0);
      }
      buffer.writeShort(pathLength);
      buffer.writeBytes(path);
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
  public boolean idempotent() {
    return true;
  }
}

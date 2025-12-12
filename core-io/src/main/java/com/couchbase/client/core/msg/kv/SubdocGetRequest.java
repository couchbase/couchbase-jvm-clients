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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreSubdocGetCommand;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.CompositeByteBuf;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Status;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.cas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.datatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeSubDocumentStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.mapSubDocumentError;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.request;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.status;
import static com.couchbase.client.core.msg.kv.SubdocUtil.handleNonFieldLevelErrors;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparing;

public class SubdocGetRequest extends BaseKeyValueRequest<SubdocGetResponse> {

  private static final byte SUBDOC_FLAG_XATTR_PATH = (byte) 0x04;
  private static final byte SUBDOC_FLAG_BINARY_VALUE = (byte) 0x20;

  private static final Comparator<Command> xattrsFirst = comparing(it -> !it.xattr());

  private final byte flags;
  private final List<Command> commands;
  private final String origKey;

  public static SubdocGetRequest create(final Duration timeout, final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                                        final RetryStrategy retryStrategy, final String key,
                                        final byte flags, final List<CoreSubdocGetCommand> commands, final RequestSpan span) {
    return new SubdocGetRequest(timeout, ctx, collectionIdentifier, retryStrategy, key, flags, convertCommands(commands), span);
  }

  public SubdocGetRequest(final Duration timeout, final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                          final RetryStrategy retryStrategy, final String key,
                          final byte flags, final List<Command> commands, final RequestSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    this.flags = flags;
    this.commands = commands;
    this.origKey = key;

    if (span != null && !CbTracing.isInternalSpan(span)) {
      ctx.coreResources().tracingDecorator().provideLowCardinalityAttr(TracingAttribute.OPERATION, span, TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN);
    }
  }

  static List<Command> convertCommands(List<CoreSubdocGetCommand> commands) {
    List<SubdocGetRequest.Command> result = new ArrayList<>(commands.size());
    for (int i = 0, len = commands.size(); i < len; i++) {
      CoreSubdocGetCommand core = commands.get(i);
      result.add(new SubdocGetRequest.Command(
        core.type(),
        core.path(),
        core.xattr(),
        core.binary(),
        i
      ));
    }

    // xattrs must come first. decode() puts the results back in original order.
    result.sort(xattrsFirst);
    return result;
  }

  @Stability.Internal
  public static List<CoreSubdocGetCommand> convertCommandsToCore(List<Command> commands) {
    List<CoreSubdocGetCommand> result = new ArrayList<>(commands.size());
    for (Command cmd : commands) {
      result.add(new CoreSubdocGetCommand(
        cmd.type,
        cmd.path,
        cmd.xattr,
        cmd.binary
      ));
    }
    return result;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    ByteBuf key = null;
    ByteBuf extras = null;
    ByteBuf body = null;

    try {
      if (!ctx.vattrEnabled()) {
        // Server will not handle all vattrs perfectly: it will reject those it doesn't know by breaking the connection.
        // Do a check to see if all vattr commands meet a whitelist of vattrs.
        for (Command c: commands) {
          if (c.xattr()
                  && (c.path.length() > 0 && c.path.charAt(0) == '$')
                  && !(c.path.startsWith("$document") || c.path.startsWith("$XTOC"))) {
            throw mapSubDocumentError(this, SubDocumentOpResponseStatus.XATTR_UNKNOWN_VATTR, c.path, c.originalIndex(), null);
          }
        }
      }

      key = encodedKeyWithCollection(alloc, ctx);

      if (flags != 0) {
        extras = alloc.buffer(Byte.BYTES).writeByte(flags);
      }

      if (commands.size() == 1) {
        // Note currently the only subdoc error response handled is ERR_SUBDOC_MULTI_PATH_FAILURE.  Make sure to
        // add the others if do the single lookup optimisation.
        // Update: single subdoc optimization will not be supported.  It adds just 3 bytes to the package size and gives
        // minimal performance gains, in return for additional client complexity.
        body = commands.get(0).encode(alloc, ctx.subdocBinaryXattr());
      } else {
        body = alloc.compositeBuffer(commands.size());
        for (Command command : commands) {
          ByteBuf commandBuffer = command.encode(alloc, ctx.subdocBinaryXattr());
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
  public SubdocGetResponse decode(final ByteBuf response, KeyValueChannelContext ctx) {
    short rawStatus = status(response);
    boolean isJson = (datatype(response) & MemcacheProtocol.Datatype.JSON.datatype()) != 0;
    Optional<ByteBuf> maybeBody = body(response);
    MemcacheProtocol.FlexibleExtras flexibleExtras = MemcacheProtocol.flexibleExtras(response);
    ResponseStatus status = decodeStatus(response);

    SubDocumentField[] values = null;
    List<CouchbaseException> errors = null;
    Optional<CouchbaseException> error = Optional.empty();
    String bodyErrorMessage = null;

    // Sources for sub-doc error-handling:
    // https://github.com/couchbase/kv_engine/blob/master/docs/SubDocument.md
    // Internal discussion: https://couchbase.slack.com/archives/CFJDXSGUA/p1694617140032519
    // The body (if present) either contains a sub-doc response (can include errors), or a JSON error message.
    boolean isSuccess = rawStatus == Status.SUCCESS.status()
      || rawStatus == Status.SUBDOC_SUCCESS_DELETED_DOCUMENT.status()
      // With lookupIn, even if some operations fail, others can succeed and the op can be seen as successful.
      || rawStatus == Status.SUBDOC_MULTI_PATH_FAILURE.status()
      || rawStatus == Status.SUBDOC_MULTI_PATH_FAILURE_DELETED.status();

    if (maybeBody.isPresent()) {
      ByteBuf body = maybeBody.get();
      if (isSuccess) {
        values = new SubDocumentField[commands.size()];
        for (Command command : commands) {
          short statusRaw = body.readShort();
          SubDocumentOpResponseStatus fieldStatus = decodeSubDocumentStatus(statusRaw);
          Optional<CouchbaseException> fieldError = Optional.empty();
          if (fieldStatus != SubDocumentOpResponseStatus.SUCCESS) {
            if (errors == null) errors = new ArrayList<>();
            CouchbaseException err = mapSubDocumentError(this, fieldStatus, command.path, command.originalIndex(), flexibleExtras);
            errors.add(err);
            fieldError = Optional.of(err);
          }
          int valueLength = body.readInt();
          byte[] value = new byte[valueLength];
          body.readBytes(value, 0, valueLength);
          SubDocumentField op = new SubDocumentField(fieldStatus, fieldError, value, command.path, command.type);
          values[command.originalIndex] = op;
        }
      } else if (isJson) {
        // Body contains an error message
        byte[] value = new byte[body.readableBytes()];
        body.readBytes(value, 0, body.readableBytes());
        bodyErrorMessage = new String(value, UTF_8);
      }
    }

    boolean isDeleted = rawStatus == Status.SUBDOC_MULTI_PATH_FAILURE_DELETED.status()
            || rawStatus == Status.SUBDOC_SUCCESS_DELETED_DOCUMENT.status();

    // Note that we send all subdoc requests as multi currently so always get this back on error
    if (rawStatus == Status.SUBDOC_MULTI_PATH_FAILURE.status()
        || rawStatus == Status.SUBDOC_MULTI_PATH_FAILURE_DELETED.status()) {
      // Special case logic for CMD_EXISTS
      if (commands.size() == 1 && commands.get(0).type == SubdocCommandType.EXISTS) {
        status = ResponseStatus.SUCCESS;
      } else {
        // Otherwise return success, as some of the operations have succeeded
        status = ResponseStatus.SUCCESS;
      }
    }

    CouchbaseException nonFieldError = handleNonFieldLevelErrors(this, rawStatus, flexibleExtras, bodyErrorMessage);
    if (nonFieldError != null) {
      error = Optional.of(nonFieldError);
    }

    return new SubdocGetResponse(status, error, values, cas(response), isDeleted, flexibleExtras);
  }

  public static class Command {
    private final SubdocCommandType type;
    private final String path;
    private final boolean xattr;
    private final int originalIndex;
    private final boolean binary;

    public Command(SubdocCommandType type, String path, boolean xattr, int originalIndex) {
      this(type, path, xattr, false, originalIndex);
    }

    public Command(SubdocCommandType type, String path, boolean xattr, boolean binary, int originalIndex) {
      this.type = type;
      this.path = path;
      this.xattr = xattr;
      this.originalIndex = originalIndex;
      this.binary = binary;
    }

    public ByteBuf encode(ByteBufAllocator alloc, boolean binarySupported) {
      byte[] path = this.path.getBytes(UTF_8);
      int pathLength = path.length;

      ByteBuf buffer = alloc.buffer(4 + pathLength);
      buffer.writeByte(type.opcode());
      byte flags = 0;
      if (xattr) {
        flags |= SUBDOC_FLAG_XATTR_PATH;
      }
      if (binary && binarySupported) {
        flags |= SUBDOC_FLAG_BINARY_VALUE;
      }
      buffer.writeByte(flags);
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

  @Override
  public String name() {
    return "lookup_in";
  }

}

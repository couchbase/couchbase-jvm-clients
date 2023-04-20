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
import com.couchbase.client.core.api.kv.CoreSubdocGetCommand;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.CompositeByteBuf;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.error.context.SubDocumentErrorContext;
import com.couchbase.client.core.error.subdoc.DocumentNotJsonException;
import com.couchbase.client.core.error.subdoc.DocumentTooDeepException;
import com.couchbase.client.core.error.subdoc.XattrInvalidKeyComboException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.FlexibleExtras;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Status;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.cas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeSubDocumentStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.mapSubDocumentError;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.request;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.status;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparing;

public class SubdocGetRequest extends BaseKeyValueRequest<SubdocGetResponse> {

  private static final byte SUBDOC_FLAG_XATTR_PATH = (byte) 0x04;

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

    if (span != null) {
      span.lowCardinalityAttribute(TracingIdentifiers.ATTR_OPERATION, TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN);
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
        i
      ));
    }

    // xattrs must come first. decode() puts the results back in original order.
    result.sort(xattrsFirst);
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
  public SubdocGetResponse decode(final ByteBuf response, KeyValueChannelContext ctx) {
    short rawStatus = status(response);
    Optional<ByteBuf> maybeBody = body(response);
    MemcacheProtocol.FlexibleExtras flexibleExtras = MemcacheProtocol.flexibleExtras(response);

    checkCommandLimit(rawStatus, maybeBody, flexibleExtras);

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
          CouchbaseException err = mapSubDocumentError(this, status, command.path, command.originalIndex(), flexibleExtras);
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

    ResponseStatus status = decodeStatus(response);
    boolean isDeleted = rawStatus == Status.SUBDOC_MULTI_PATH_FAILURE_DELETED.status()
            || rawStatus == Status.SUBDOC_SUCCESS_DELETED_DOCUMENT.status();

    Optional<CouchbaseException> error = Optional.empty();

    // Note that we send all subdoc requests as multi currently so always get this back on error
    if (rawStatus == Status.SUBDOC_MULTI_PATH_FAILURE.status()
        || rawStatus == Status.SUBDOC_MULTI_PATH_FAILURE_DELETED.status()) {
      // Special case logic for CMD_EXISTS
      if (commands.size() == 1 && commands.get(0).type == SubdocCommandType.EXISTS) {
        status = ResponseStatus.SUCCESS;
      }
      // If a single subdoc op was tried and failed, return that directly
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
      SubDocumentErrorContext e = createSubDocumentExceptionContext(SubDocumentOpResponseStatus.DOC_NOT_JSON, flexibleExtras);
      error = Optional.of(new DocumentNotJsonException(e));
    } else if (rawStatus == Status.SUBDOC_DOC_TOO_DEEP.status()) {
      SubDocumentErrorContext e = createSubDocumentExceptionContext(SubDocumentOpResponseStatus.DOC_TOO_DEEP, flexibleExtras);
      error = Optional.of(new DocumentTooDeepException(e));
    } else if (rawStatus == Status.SUBDOC_XATTR_INVALID_KEY_COMBO.status()) {
      SubDocumentErrorContext e = createSubDocumentExceptionContext(SubDocumentOpResponseStatus.XATTR_INVALID_KEY_COMBO, flexibleExtras);
      error = Optional.of(new XattrInvalidKeyComboException(e));
    }

    // Do not handle SUBDOC_INVALID_COMBO here, it indicates a client-side bug
    return new SubdocGetResponse(status, error, values, cas(response), isDeleted, flexibleExtras);
  }

  private void checkCommandLimit(short rawStatus, Optional<ByteBuf> maybeBody, FlexibleExtras flexibleExtras) {
    if (rawStatus == Status.SUBDOC_INVALID_COMBO.status()) {
      // Assume we are not sending lookups and mutations in the same request.
      // The server also uses this error code when there are too many commands.
      String msg = "Sub-document lookup failed with error code INVALID_COMBO," +
        " which probably means too many sub-document operations in a single request.";

      if (maybeBody.isPresent()) {
        // The body includes the actual sub-doc operation limit.
        // Include it in the exception message, but only if it looks like a JSON object.
        String body = maybeBody.get().toString(UTF_8);
        if (body.startsWith("{")) {
          msg += " Server said: " + body;
        }
      }

      ErrorContext errorContext = createSubDocumentExceptionContext(
        SubDocumentOpResponseStatus.INVALID_COMBO,
        flexibleExtras
      );

      throw new InvalidArgumentException(msg, null, errorContext);
    }
  }

  private SubDocumentErrorContext createSubDocumentExceptionContext(SubDocumentOpResponseStatus status,
                                                                    @Nullable MemcacheProtocol.FlexibleExtras flexibleExtras) {
    return new SubDocumentErrorContext(
      KeyValueErrorContext.completedRequest(this, ResponseStatus.SUBDOC_FAILURE, flexibleExtras),
      0,
      null,
      status,
      null
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

  @Override
  public String name() {
    return "lookup_in";
  }

}

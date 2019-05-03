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
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException;
import com.couchbase.client.core.error.subdoc.DocumentNotJsonException;
import com.couchbase.client.core.error.subdoc.DocumentTooDeepException;
import com.couchbase.client.core.error.subdoc.MultiMutationException;
import com.couchbase.client.core.error.subdoc.SubDocumentException;
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.CompositeByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SubdocMutateRequest extends BaseKeyValueRequest<SubdocMutateResponse> {

  private static final byte SUBDOC_FLAG_XATTR_PATH = (byte) 0x04;
  private static final byte SUBDOC_FLAG_CREATE_PATH = (byte) 0x01;
  private static final byte SUBDOC_FLAG_EXPAND_MACRO = (byte) 0x10;

  private static final byte SUBDOC_DOC_FLAG_MKDOC = (byte) 0x01;
  private static final byte SUBDOC_DOC_FLAG_ADD = (byte) 0x02;
  private static final byte SUBDOC_DOC_FLAG_ACCESS_DELETED = (byte) 0x04;

  public static final int SUBDOC_MAX_FIELDS = 16;

  private final byte flags;
  private final long expiration;
  private final long cas;
  private final List<Command> commands;
  private final String origKey;
  private final Optional<DurabilityLevel> syncReplicationType;


  public SubdocMutateRequest(final Duration timeout, final CoreContext ctx, final String bucket,
                             final RetryStrategy retryStrategy, final String key,
                             final byte[] collection,
                             final boolean insertDocument, final boolean upsertDocument,
                             final List<Command> commands, long expiration, long cas,
                             final Optional<DurabilityLevel> syncReplicationType) {
    super(timeout, ctx, bucket, retryStrategy, key, collection);
    byte flags = 0;

    if (insertDocument && upsertDocument) {
      throw new IllegalArgumentException("Cannot both insert and upsert full document");
    }

    if (upsertDocument) {
      flags |= SUBDOC_DOC_FLAG_MKDOC;
    }

    if (insertDocument) {
      flags |= SUBDOC_DOC_FLAG_ADD;
    }

    this.flags = flags;
    this.commands = commands;
    this.expiration = expiration;
    this.cas = cas;
    this.origKey = key;
    this.syncReplicationType = syncReplicationType;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, ChannelContext ctx) {
    ByteBuf key = Unpooled.wrappedBuffer(ctx.collectionsEnabled() ? keyWithCollection() : key());

    ByteBuf extras = alloc.buffer();
    if (flags != 0) {
      extras.writeByte(flags);
    }
    if (expiration != 0) {
      extras.writeInt((int) expiration);
    }

    ByteBuf body;
    if (commands.size() == 1) {
      body = commands.get(0).encode(alloc);
    } else {
      body = alloc.compositeBuffer(commands.size());
      for (Command command : commands) {
        ByteBuf commandBuffer = command.encode(alloc);
        ((CompositeByteBuf) body).addComponent(commandBuffer);
        body.writerIndex(body.writerIndex() + commandBuffer.readableBytes());
      }
    }

    ByteBuf request;
    if (syncReplicationType.isPresent()) {
      if (ctx.syncReplicationEnabled()) {
        ByteBuf flexibleExtras = flexibleSyncReplication(alloc, syncReplicationType.get(), timeout());
        request = flexibleRequest(alloc, Opcode.SUBDOC_MULTI_MUTATE, noDatatype(), partition(), opaque,
                noCas(), flexibleExtras, extras, key, body);
        flexibleExtras.release();
      }
      else {
        throw new DurabilityLevelNotAvailableException(syncReplicationType.get());
      }
    } else {
      request = request(alloc, Opcode.SUBDOC_MULTI_MUTATE, noDatatype(), partition(), opaque,
        cas, extras, key, body);
    }

    extras.release();
    key.release();
    body.release();
    return request;
  }

  @Override
  public SubdocMutateResponse decode(final ByteBuf response, ChannelContext ctx) {
    Optional<ByteBuf> maybeBody = body(response);
    short rawOverallStatus = status(response);
    ResponseStatus overallStatus = decodeStatus(response);
    Optional<SubDocumentException> error = Optional.empty();

    List<SubdocField> values;

    if (maybeBody.isPresent()) {
      ByteBuf body = maybeBody.get();

      // If there's a multi-mutation failure we only get the first failure back
      if (rawOverallStatus == Status.SUBDOC_MULTI_PATH_FAILURE.status()) {
        byte index = body.readByte();
        short opStatusRaw = body.readShort();
        SubDocumentOpResponseStatus opStatus = decodeSubDocumentStatus(opStatusRaw);
        SubDocumentException err = mapSubDocumentError(opStatus, commands.get(index).path, origKey);
        error = Optional.of(new MultiMutationException(index, opStatus, err));
        values = new ArrayList<>();
      }
      else {
        values = new ArrayList<>(commands.size());
        for (Command command : commands) {
          byte index = body.readByte();
          short statusRaw = body.readShort();
          SubDocumentOpResponseStatus status = decodeSubDocumentStatus(statusRaw);

          // The status here should always be SUCCESS
          if (status != SubDocumentOpResponseStatus.SUCCESS) {
            SubDocumentException err = mapSubDocumentError(status, command.path, origKey);

            SubdocField op = new SubdocField(status, Optional.of(err), new byte[] {}, command.path, command.type);
            values.add(op);
          }
          else {
            int valueLength = body.readInt();
            byte[] value = new byte[valueLength];
            body.readBytes(value, 0, valueLength);
            SubdocField op = new SubdocField(status, Optional.empty(), value, command.path, command.type);
            values.add(op);
          }
        }
      }
    } else {
      values = new ArrayList<>();
    }


    // Note that we send all subdoc requests as multi currently so always get this back on error
    if (rawOverallStatus == Status.SUBDOC_DOC_NOT_JSON.status()) {
      error = Optional.of(new DocumentNotJsonException(origKey));
    } else if (rawOverallStatus == Status.SUBDOC_DOC_TOO_DEEP.status()) {
      error = Optional.of(new DocumentTooDeepException(origKey));
    }
    // Do not handle SUBDOC_INVALID_COMBO here, it indicates a client-side bug


    return new SubdocMutateResponse(
      overallStatus,
      error,
      values,
      cas(response),
      extractToken(ctx.mutationTokensEnabled(), partition(), response, ctx.bucket())
    );
  }

  public static RuntimeException errIfNoCommands() {
    return new IllegalArgumentException("No SubDocument commands provided");
  }

  public static RuntimeException errIfTooManyCommands() {
    return new IllegalArgumentException("A maximum of " + SubdocMutateRequest.SUBDOC_MAX_FIELDS + " fields can be provided");
  }

  public static class Command {

    private final SubdocCommandType type;
    private final String path;
    private final byte[] fragment;
    private final boolean createParent;
    private final boolean xattr;
    private final boolean expandMacro;

      public Command(SubdocCommandType type, String path, byte[] fragment,
                   boolean createParent, boolean xattr, boolean expandMacro) {
      this.type = type;
      this.path = path;
      this.xattr = xattr;
      this.fragment = fragment;
      this.createParent = createParent;
      this.expandMacro = expandMacro;
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
  }

}

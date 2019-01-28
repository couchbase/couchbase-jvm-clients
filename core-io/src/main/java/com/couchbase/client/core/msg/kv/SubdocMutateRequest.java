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
import com.couchbase.client.core.io.netty.kv.EncodeContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.retry.RetryStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;

public class SubdocMutateRequest extends BaseKeyValueRequest<SubdocMutateResponse> {

  private static final byte SUBDOC_FLAG_XATTR_PATH = (byte) 0x04;
  private static final byte SUBDOC_FLAG_CREATE_PATH = (byte) 0x01;

  private final byte flags;
  private final long expiration;
  private final List<Command> commands;

  public SubdocMutateRequest(final Duration timeout, final CoreContext ctx, final String bucket,
                             final RetryStrategy retryStrategy, final String key,
                             final byte[] collection, final byte flags, final List<Command> commands, long expiration) {
    super(timeout, ctx, bucket, retryStrategy, key, collection);
    this.flags = flags;
    this.commands = commands;
    this.expiration = expiration;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, EncodeContext ctx) {
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
      // todo: Optimize into single get request only?
      body = commands.get(0).encode(alloc);
    } else {
      body = alloc.compositeBuffer(commands.size());
      for (Command command : commands) {
        ByteBuf commandBuffer = command.encode(alloc);
        ((CompositeByteBuf) body).addComponent(commandBuffer);
        body.writerIndex(body.writerIndex() + commandBuffer.readableBytes());
      }
    }

    ByteBuf request = request(alloc, Opcode.SUBDOC_MULTI_MUTATE, noDatatype(), partition(), opaque,
      noCas(), extras, key, body);

    extras.release();
    key.release();
    body.release();
    return request;
  }

  @Override
  public SubdocMutateResponse decode(final ByteBuf response) {
    Optional<ByteBuf> maybeBody = body(response);
    List<SubdocMutateResponse.ResponseValue> values;
    if (maybeBody.isPresent()) {
      ByteBuf body = maybeBody.get();
      values = new ArrayList<>(commands.size());
      for (Command command : commands) {
        short status = body.readShort();
        int valueLength = body.readInt();
        byte[] value = new byte[valueLength];
        body.readBytes(value, 0, valueLength);
        values.add(new SubdocMutateResponse.ResponseValue(status, value, command.path));
      }
    } else {
      values = new ArrayList<>();
    }
    return new SubdocMutateResponse(decodeStatus(response), values, cas(response), Optional.empty());
  }

  public static class Command {

    private final CommandType type;
    private final String path;
    private final byte[] fragment;
    private final boolean createParent;
    private final boolean xattr;

    public Command(CommandType type, String path, byte[] fragment, boolean createParent, boolean xattr) {
      this.type = type;
      this.path = path;
      this.xattr = xattr;
      this.fragment = fragment;
      this.createParent = createParent;
    }

    public ByteBuf encode(final ByteBufAllocator alloc) {
      byte[] path = this.path.getBytes(CharsetUtil.UTF_8);
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
      buffer.writeByte(flags);
      buffer.writeShort(pathLength);
      buffer.writeInt(fragment.length);
      buffer.writeBytes(path);
      buffer.writeBytes(fragment);

      return buffer;
    }
  }

  public enum CommandType {
    UPSERTDOC((byte) 0x01),
    COUNTER((byte) 0xcf),
    REPLACE((byte) 0xca),
    DICT_ADD((byte) 0xc7),
    DICT_UPSERT((byte) 0xc8),
    ARRAY_PUSH_FIRST((byte) 0xcc),
    ARRAY_PUSH_LAST((byte) 0xcb),
    ARRAY_ADD_UNIQUE((byte) 0xce),
    ARRAY_INSERT((byte) 0xcd),
    DELETE((byte) 0xc9);

    private final byte opcode;

    CommandType(byte opcode) {
      this.opcode = opcode;
    }

    byte opcode() {
      return opcode;
    }
  }
}

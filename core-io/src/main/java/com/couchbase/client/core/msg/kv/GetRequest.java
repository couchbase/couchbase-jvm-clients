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

import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.ResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.time.Duration;
import java.util.function.Function;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;

/**
 * Represents a KV Get operation.
 *
 * @since 2.0.0
 */
public class GetRequest extends BaseKeyValueRequest<GetResponse> {

  private final byte[] key;

  public GetRequest(final byte[] key, final Duration timeout, final RequestContext ctx) {
    super(timeout, ctx);
    this.key = key;
  }

  public byte[] key() {
    return key;
  }

  @Override
  public ByteBuf encode(final ByteBufAllocator alloc, final int opaque) {
    ByteBuf key = Unpooled.wrappedBuffer(key());
    ByteBuf r = MemcacheProtocol.request(
      alloc,
      MemcacheProtocol.Opcode.GET,
      noDatatype(),
      partition(),
      opaque,
      noCas(),
      noExtras(),
      key,
      noBody());
    key.release();
    return r;
  }

  @Override
  public GetResponse decode(final ByteBuf response) {
    ResponseStatus status = MemcacheProtocol.decodeStatus(response);
    System.err.println(ByteBufUtil.prettyHexDump(response));
    System.err.println(ByteBufUtil.prettyHexDump(body(response).get()));
    byte[] content = status.success()
      ? body(response).map(ByteBufUtil::getBytes).orElse(new byte[] {})
      : null;
    return new GetResponse(status, content);
  }

}

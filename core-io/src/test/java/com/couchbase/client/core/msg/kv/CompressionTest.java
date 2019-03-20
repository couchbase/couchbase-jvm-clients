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
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.util.Utils;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.datatype;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Verifies that the supported compression operations either do not compress or compress based
 * on the configuration provided.
 */
class CompressionTest {

  private final Duration timeout = Duration.ZERO;
  private final CoreContext coreContext = mock(CoreContext.class);
  private final String bucket = "bucket";
  private final String key = "key";
  private final RetryStrategy retryStrategy = BestEffortRetryStrategy.INSTANCE;
  private final byte[] collection = null;
  private final long cas = 0;
  private final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
  private final long expiry = 0;
  private final int flags = 0;
  private final Optional<DurabilityLevel> durability = Optional.empty();

  private final byte[] shortContent = "short".getBytes(UTF_8);
  private final byte[] longContent = Utils.readResource(
    "dummy.json",
    CompressionTest.class
  ).getBytes(UTF_8);

  @Test
  void doesNotCompressIfDisabledAppend() {
    AppendRequest request = new AppendRequest(timeout, coreContext, bucket, retryStrategy, key,
      collection, longContent, cas, durability);

    ByteBuf encoded = request.encode(allocator, 0, ctx(false));
    assertEquals(0, datatype(encoded));
    assertEquals(Unpooled.wrappedBuffer(longContent), body(encoded).get());

    ReferenceCountUtil.release(encoded);
  }

  @Test
  void doesNotCompressIfTooShortAppend() {
    AppendRequest request = new AppendRequest(timeout, coreContext, bucket, retryStrategy, key,
      collection, shortContent, cas, durability);

    ByteBuf encoded = request.encode(allocator, 0, ctx(true));
    assertEquals(0, datatype(encoded));
    assertEquals(Unpooled.wrappedBuffer(shortContent), body(encoded).get());

    ReferenceCountUtil.release(encoded);
  }

  @Test
  void doesCompressLongAppend() {
    AppendRequest request = new AppendRequest(timeout, coreContext, bucket, retryStrategy, key,
      collection, longContent, cas, durability);

    ByteBuf encoded = request.encode(allocator, 0, ctx(true));
    assertEquals(MemcacheProtocol.Datatype.SNAPPY.datatype(), datatype(encoded));
    assertTrue(body(encoded).get().readableBytes() < longContent.length);

    ReferenceCountUtil.release(encoded);
  }

  @Test
  void doesNotCompressIfDisabledPrepend() {
    PrependRequest request = new PrependRequest(timeout, coreContext, bucket, retryStrategy, key,
      collection, longContent, cas, durability);

    ByteBuf encoded = request.encode(allocator, 0, ctx(false));
    assertEquals(0, datatype(encoded));
    assertEquals(Unpooled.wrappedBuffer(longContent), body(encoded).get());

    ReferenceCountUtil.release(encoded);
  }

  @Test
  void doesNotCompressIfTooShortPrepend() {
    PrependRequest request = new PrependRequest(timeout, coreContext, bucket, retryStrategy, key,
      collection, shortContent, cas, durability);

    ByteBuf encoded = request.encode(allocator, 0, ctx(true));
    assertEquals(0, datatype(encoded));
    assertEquals(Unpooled.wrappedBuffer(shortContent), body(encoded).get());

    ReferenceCountUtil.release(encoded);
  }

  @Test
  void doesCompressLongPrepend() {
    PrependRequest request = new PrependRequest(timeout, coreContext, bucket, retryStrategy, key,
      collection, longContent, cas, durability);

    ByteBuf encoded = request.encode(allocator, 0, ctx(true));
    assertEquals(MemcacheProtocol.Datatype.SNAPPY.datatype(), datatype(encoded));
    assertTrue(body(encoded).get().readableBytes() < longContent.length);

    ReferenceCountUtil.release(encoded);
  }

  @Test
  void doesNotCompressIfDisabledInsert() {
    InsertRequest request = new InsertRequest(key, collection, longContent, expiry, flags, timeout,
      coreContext, bucket, retryStrategy, durability);

    ByteBuf encoded = request.encode(allocator, 0, ctx(false));
    assertEquals(0, datatype(encoded));
    assertEquals(Unpooled.wrappedBuffer(longContent), body(encoded).get());

    ReferenceCountUtil.release(encoded);
  }

  @Test
  void doesNotCompressIfTooShortInsert() {
    InsertRequest request = new InsertRequest(key, collection, shortContent, expiry, flags, timeout,
      coreContext, bucket, retryStrategy, durability);

    ByteBuf encoded = request.encode(allocator, 0, ctx(true));
    assertEquals(0, datatype(encoded));
    assertEquals(Unpooled.wrappedBuffer(shortContent), body(encoded).get());

    ReferenceCountUtil.release(encoded);
  }

  @Test
  void doesCompressLongInsert() {
    InsertRequest request = new InsertRequest(key, collection, longContent, expiry, flags, timeout,
      coreContext, bucket, retryStrategy, durability);

    ByteBuf encoded = request.encode(allocator, 0, ctx(true));
    assertEquals(MemcacheProtocol.Datatype.SNAPPY.datatype(), datatype(encoded));
    assertTrue(body(encoded).get().readableBytes() < longContent.length);

    ReferenceCountUtil.release(encoded);
  }

  @Test
  void doesNotCompressIfDisabledUpsert() {
    UpsertRequest request = new UpsertRequest(key, collection, longContent, expiry, flags, timeout,
      coreContext, bucket, retryStrategy, Optional.empty());

    ByteBuf encoded = request.encode(allocator, 0, ctx(false));
    assertEquals(0, datatype(encoded));
    assertEquals(Unpooled.wrappedBuffer(longContent), body(encoded).get());

    ReferenceCountUtil.release(encoded);
  }

  @Test
  void doesNotCompressIfTooShortUpsert() {
    UpsertRequest request = new UpsertRequest(key, collection, shortContent, expiry, flags, timeout,
      coreContext, bucket, retryStrategy, Optional.empty());

    ByteBuf encoded = request.encode(allocator, 0, ctx(true));
    assertEquals(0, datatype(encoded));
    assertEquals(Unpooled.wrappedBuffer(shortContent), body(encoded).get());

    ReferenceCountUtil.release(encoded);
  }

  @Test
  void doesCompressLongUpsert() {
    UpsertRequest request = new UpsertRequest(key, collection, longContent, expiry, flags, timeout,
      coreContext, bucket, retryStrategy, Optional.empty());

    ByteBuf encoded = request.encode(allocator, 0, ctx(true));
    assertEquals(MemcacheProtocol.Datatype.SNAPPY.datatype(), datatype(encoded));
    assertTrue(body(encoded).get().readableBytes() < longContent.length);

    ReferenceCountUtil.release(encoded);
  }

  @Test
  void doesNotCompressIfDisabledReplace() {
    ReplaceRequest request = new ReplaceRequest(key, collection, longContent, expiry, flags, timeout,
      cas, coreContext, bucket, retryStrategy, durability);

    ByteBuf encoded = request.encode(allocator, 0, ctx(false));
    assertEquals(0, datatype(encoded));
    assertEquals(Unpooled.wrappedBuffer(longContent), body(encoded).get());

    ReferenceCountUtil.release(encoded);
  }

  @Test
  void doesNotCompressIfTooShortReplace() {
    ReplaceRequest request = new ReplaceRequest(key, collection, shortContent, expiry, flags, timeout,
      cas, coreContext, bucket, retryStrategy, durability);

    ByteBuf encoded = request.encode(allocator, 0, ctx(true));
    assertEquals(0, datatype(encoded));
    assertEquals(Unpooled.wrappedBuffer(shortContent), body(encoded).get());

    ReferenceCountUtil.release(encoded);
  }

  @Test
  void doesCompressLongReplace() {
    ReplaceRequest request = new ReplaceRequest(key, collection, longContent, expiry, flags, timeout,
      cas, coreContext, bucket, retryStrategy, durability);

    ByteBuf encoded = request.encode(allocator, 0, ctx(true));
    assertEquals(MemcacheProtocol.Datatype.SNAPPY.datatype(), datatype(encoded));
    assertTrue(body(encoded).get().readableBytes() < longContent.length);

    ReferenceCountUtil.release(encoded);
  }

  private ChannelContext ctx(boolean enabled) {
    return new ChannelContext(
      CompressionConfig.builder().enabled(enabled).build(),
      false,
      false,
      bucket,
      false,
      false
    );
  }

}

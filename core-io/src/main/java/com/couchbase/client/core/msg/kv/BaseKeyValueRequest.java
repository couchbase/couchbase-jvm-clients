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
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.ErrorMap;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.UnsignedLEB128;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * The {@link BaseKeyValueRequest} should be subclassed by all KeyValue requests since it
 * provides common ground for all of them (i.e. adding the kv partition needed).
 *
 * @since 2.0.0
 */
public abstract class BaseKeyValueRequest<R extends Response>
  extends BaseRequest<R>
  implements KeyValueRequest<R> {

  private static byte[] EMPTY_KEY = new byte[] {};
  private final String bucket;
  private final byte[] key;
  private final byte[] collection;

  /**
   * Once set, stores the partition where this request should be dispatched against.
   */
  private volatile short partition;

  BaseKeyValueRequest(final Duration timeout, final CoreContext ctx, final String bucket,
                      final RetryStrategy retryStrategy, String key, byte[] collection) {
    super(timeout, ctx, retryStrategy);
    this.bucket = bucket;
    this.key = encodeKey(key);
    this.collection = collection;
  }

  @Override
  public short partition() {
    return partition;
  }

  @Override
  public void partition(short partition) {
    this.partition = partition;
  }

  /**
   * Returns the encoded version of the key in UTF-8.
   *
   * @param key the key to encode.
   * @return the encoded key.
   */
  static byte[] encodeKey(final String key) {
    return key == null || key.isEmpty() ? EMPTY_KEY : key.getBytes(CharsetUtil.UTF_8);
  }

  protected byte[] keyWithCollection() {
    byte[] encoded = new byte[key.length + collection.length];
    System.arraycopy(collection, 0, encoded, 0, collection.length);
    System.arraycopy(key, 0, encoded, collection.length, key.length);
    return encoded;
  }

  @Override
  public ServiceType serviceType() {
    return ServiceType.KV;
  }

  @Override
  public Map<String, Object> serviceContext() {
    Map<String, Object> ctx = new HashMap<>();
    ctx.put("type", serviceType().ident());
    if (bucket != null) {
      ctx.put("bucket", bucket);
    }
    if (key != null) {
      ctx.put("key", new String(key, CharsetUtil.UTF_8));
    }
    if (collection != null) {
      ctx.put("cid", UnsignedLEB128.decode(collection));
    }
    return ctx;
  }

  @Override
  public String bucket() {
    return bucket;
  }

  @Override
  public byte[] key() {
    return key;
  }
}

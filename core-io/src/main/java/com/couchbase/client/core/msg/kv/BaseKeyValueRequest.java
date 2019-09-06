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
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.error.CollectionDoesNotExistException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.CollectionMap;
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.Bytes;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The {@link BaseKeyValueRequest} should be subclassed by all KeyValue requests since it
 * provides common ground for all of them (i.e. adding the kv partition needed).
 *
 * @since 2.0.0
 */
public abstract class BaseKeyValueRequest<R extends Response>
  extends BaseRequest<R>
  implements KeyValueRequest<R> {

  private final byte[] key;
  private final CollectionIdentifier collectionIdentifier;

  /**
   * Once set, stores the partition where this request should be dispatched against.
   */
  private volatile short partition;

  BaseKeyValueRequest(final Duration timeout, final CoreContext ctx, final RetryStrategy retryStrategy,
                      final String key, final CollectionIdentifier collectionIdentifier) {
    super(timeout, ctx, retryStrategy);
    this.key = encodeKey(key);
    this.collectionIdentifier = collectionIdentifier;
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
    return key == null || key.isEmpty() ? Bytes.EMPTY_BYTE_ARRAY : key.getBytes(UTF_8);
  }

  /**
   * This method with return an encoded key with or without the collection prefix, depending on the
   * context provided.
   *
   * @param alloc the buffer allocator to use.
   * @param ctx the channel context.
   * @return the encoded ID, maybe with the collection prefix in place.
   */
  protected ByteBuf encodedKeyWithCollection(final ByteBufAllocator alloc, final ChannelContext ctx) {
    if (ctx.collectionsEnabled()) {
      byte[] collection = ctx.collectionMap().get(collectionIdentifier);
      if (collection == null) {
        throw new CollectionDoesNotExistException("Collection \""
          + collectionIdentifier.collection() + "\" in scope \""
          + collectionIdentifier.scope() + "\" does not exist.");
      }

      return alloc
        .buffer(key.length + collection.length)
        .writeBytes(collection)
        .writeBytes(key);
    } else {
      return alloc
        .buffer(key.length)
        .writeBytes(key);
    }
  }

  @Override
  public ServiceType serviceType() {
    return ServiceType.KV;
  }

  @Override
  public Map<String, Object> serviceContext() {
    Map<String, Object> ctx = new HashMap<>();
    ctx.put("type", serviceType().ident());

    if (collectionIdentifier != null) {
      ctx.put("bucket", redactMeta(collectionIdentifier.bucket()));
      ctx.put("scope", redactMeta(collectionIdentifier.scope().orElse(CollectionIdentifier.DEFAULT_SCOPE)));
      ctx.put("collection", redactMeta(collectionIdentifier.collection().orElse(CollectionIdentifier.DEFAULT_COLLECTION)));
    }

    if (key != null) {
      ctx.put("key", redactUser(new String(key, UTF_8)));
    }

    return ctx;
  }

  @Override
  public byte[] key() {
    return key;
  }

  @Override
  public String bucket() {
    return collectionIdentifier == null ? null : collectionIdentifier.bucket();
  }

  @Override
  public CollectionIdentifier collectionIdentifier() {
    return collectionIdentifier;
  }

}

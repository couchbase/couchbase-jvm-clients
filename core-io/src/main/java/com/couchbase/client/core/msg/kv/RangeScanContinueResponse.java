/*
 * Copyright (c) 2022 Couchbase, Inc.
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

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.kv.CoreRangeScanItem;
import com.couchbase.client.core.kv.LastCoreRangeScanItem;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.util.UnsignedLEB128;
import reactor.blockhound.shaded.net.bytebuddy.implementation.bytecode.Throw;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Instant;
import java.util.List;

import static com.couchbase.client.core.util.Validators.notNull;

public class RangeScanContinueResponse extends BaseResponse {

  private final Sinks.Many<CoreRangeScanItem> items;
  private final boolean keysOnly;

  public RangeScanContinueResponse(final ResponseStatus status, final Sinks.Many<CoreRangeScanItem> items,
                                   final boolean keysOnly) {
    super(status);
    this.items = notNull(items, "CoreRangeScanItems Sink");
    this.keysOnly = keysOnly;
  }

  public Flux<CoreRangeScanItem> items() {
    return items.asFlux();
  }

  /**
   * Method used in testing to feed items directly.
   *
   * @param items
   * @param signalLastItem
   * @param completeFeed
   */
  public void feedItems(List<CoreRangeScanItem> items, final boolean signalLastItem, final boolean completeFeed) {
    for (CoreRangeScanItem item : items) {
      this.items.tryEmitNext(item);
    }
    if (signalLastItem) {
      this.items.tryEmitNext(LastCoreRangeScanItem.INSTANCE);
    }
    if (completeFeed) {
      this.items.tryEmitComplete();
    }
  }

  /**
   * Feeds the items from the buffer into the sink and completes it if needed.
   *
   * @param itemsBuf the buffer with the items to feed.
   * @param completeFeed if the sink needs to be completed at the end.
   */
  public void feedItems(final ByteBuf itemsBuf, final boolean signalLastItem, final boolean completeFeed) {
    try {
      if (keysOnly) {
        feedKeysOnly(itemsBuf);
      } else {
        feedKeysAndBody(itemsBuf);
      }

      if (signalLastItem) {
        items.tryEmitNext(LastCoreRangeScanItem.INSTANCE);
      }
      if (completeFeed) {
        items.tryEmitComplete();
      }
    } catch (Throwable cause) {
      items.tryEmitError(cause);
    }
  }

  public void failFeed(final Throwable cause) {
    items.tryEmitError(cause);
  }

  private void feedKeysOnly(final ByteBuf itemsBuf) {
    while (itemsBuf.isReadable()) {
      int keyLength = (int) UnsignedLEB128.read(itemsBuf);
      byte[] key = new byte[(int) keyLength];
      itemsBuf.readBytes(key, 0, keyLength);
      items.tryEmitNext(CoreRangeScanItem.keyOnly(key));
    }
  }

  private void feedKeysAndBody(final ByteBuf itemsBuf) {
    while (itemsBuf.isReadable()) {
      int flags = itemsBuf.readInt();
      long expiry = itemsBuf.readUnsignedInt();
      Instant expiryInstant = expiry == 0 ? null : Instant.ofEpochSecond(expiry);
      long seqno = itemsBuf.readLong();
      long cas = itemsBuf.readLong();
      byte dataType = itemsBuf.readByte();

      byte[] key = readLengthPrefixedBytes(itemsBuf);
      byte[] rawValue = readLengthPrefixedBytes(itemsBuf);
      byte[] value = MemcacheProtocol.tryDecompression(rawValue, dataType);

      items.tryEmitNext(CoreRangeScanItem.keyAndBody(flags, expiryInstant, seqno, cas, key, value));
    }
  }

  private static byte[] readLengthPrefixedBytes(ByteBuf buf) {
    int len = Math.toIntExact(UnsignedLEB128.read(buf));
    byte[] result = new byte[(int) len];
    buf.readBytes(result);
    return result;
  }

}

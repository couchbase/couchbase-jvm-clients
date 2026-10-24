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

import com.couchbase.client.core.io.SectionReader;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.kv.CoreRangeScanItem;
import com.couchbase.client.core.kv.LastCoreRangeScanItem;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
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
   * Feeds the items from the response's body into the sink and completes it if needed.
   *
   * @param body the response's body (the items to feed).
   * @param completeFeed if the sink needs to be completed at the end.
   */
  public void feedItems(SectionReader body, boolean signalLastItem, boolean completeFeed) {
    try {
      if (keysOnly) {
        feedKeysOnly(body);
      } else {
        feedKeysAndBody(body);
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

  private void feedKeysOnly(SectionReader body) {
    while (body.remaining() > 0) {
      byte[] key = readLengthPrefixedBytes(body);
      items.tryEmitNext(CoreRangeScanItem.keyOnly(key));
    }
  }

  private void feedKeysAndBody(SectionReader body) {
    while (body.remaining() > 0) {
      int flags = body.readInt();
      long expiry = body.readUnsignedInt();
      Instant expiryInstant = expiry == 0 ? null : Instant.ofEpochSecond(expiry);
      long seqno = body.readLong();
      long cas = body.readLong();
      byte dataType = body.readByte();

      byte[] key = readLengthPrefixedBytes(body);
      byte[] rawValue = readLengthPrefixedBytes(body);
      byte[] value = MemcacheProtocol.tryDecompression(rawValue, dataType);

      items.tryEmitNext(CoreRangeScanItem.keyAndBody(flags, expiryInstant, seqno, cas, key, value));
    }
  }

  private static byte[] readLengthPrefixedBytes(SectionReader body) {
    return body.readBytes(Math.toIntExact(body.readUleb128()));
  }

}

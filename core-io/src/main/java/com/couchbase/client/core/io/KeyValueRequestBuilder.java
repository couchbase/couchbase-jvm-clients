/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.io;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.compression.snappy.SnappyCodec;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import org.jspecify.annotations.NullMarked;

/**
 * Builds one KV request packet (see {@link KeyValueRequestEncoder#beginRequest}).
 * <p>
 * A request sets the header fields it needs (each defaults to 0), its key and value (if it has them), and writes
 * its framing extras and extras with {@link #framingExtras()} and {@link #extras()}. A request that builds its
 * value (for example, from several parts) writes it with {@link #valueWriter()} instead of giving an array.
 * All of it may be done in any order: each part has its own place, and the packet is put together, in protocol
 * order, when the request is finished. A section's length is however much the request writes into it, so the
 * request never states a length. For example:
 * <pre>
 * KeyValueRequestBuilder request = encoder.beginRequest(Opcode.SET)
 *   .partition(partition())
 *   .key(collection, key)
 *   .value(content);
 * request.extras().putInt(flags).putInt(expiry);
 * </pre>
 * {@code putByte} and {@code putShort} take an int, for convenience, and accept a signed or unsigned value
 * (-128 to 255, and -32768 to 65535); anything else is rejected, rather than truncated.
 * <p>
 * The header, framing extras, extras and key may take at most {@link KeyValueRequestEncoder#MAX_REQUEST_HEAD_SIZE}
 * bytes; that, and each part's limit in the header, are checked when the request is finished. The request is
 * flexible ("alternative" framing) if it has framing extras.
 * <p>
 * The builder is reused for later requests, so don't keep it, or a section writer, after the request is finished.
 * Using either once the request's head is complete throws {@link IllegalStateException}.
 */
@Stability.Internal
@NullMarked
public interface KeyValueRequestBuilder {
  /**
   * Returns the context of the connection the request is for (the encoder's {@link KeyValueRequestEncoder#context()}),
   * for helpers that build part of a request from the builder alone.
   */
  KeyValueChannelContext context();

  /**
   * Returns the Snappy compressor for the request's value. It belongs to the encoder, which reuses it for
   * later requests (and its working memory with it); don't keep it after the request is finished.
   */
  SnappyCodec.Compressor compressor();

  /**
   * Adds datatype bits: ORs them into the datatype (which starts at 0), so parts of a request (its value's
   * compression, say) can each add their own, in any order. -128 to 255, so a signed or unsigned byte.
   *
   * @throws IllegalArgumentException if it's out of that range.
   */
  KeyValueRequestBuilder addDatatype(int datatype);

  /**
   * Sets the partition (vbucket); 0 to 65535.
   *
   * @throws IllegalArgumentException if it's out of that range.
   */
  KeyValueRequestBuilder partition(int partition);

  KeyValueRequestBuilder cas(long cas);

  /**
   * Sets the key. The array isn't copied now; it's written when the request is finished, so it must not change
   * until then.
   */
  KeyValueRequestBuilder key(byte[] key);

  /**
   * Sets the key: a document's key, after its collection prefix (see
   * {@link com.couchbase.client.core.msg.kv.BaseKeyValueRequest#collectionPrefix}). Like {@link #key(byte[])}.
   */
  KeyValueRequestBuilder key(byte[] collectionPrefix, byte[] key);

  /**
   * Sets the value (body). The array isn't copied now; it's written when the caller finishes the request,
   * so it must not change until then.
   *
   * @param length how much of {@code value} (from the start) is the value.
   */
  KeyValueRequestBuilder value(byte[] value, int length);

  /**
   * Sets all of {@code value} as the value. See {@link #value(byte[], int)}.
   */
  KeyValueRequestBuilder value(byte[] value);

  /**
   * Returns the writer of the value, for a request that builds its value instead of giving an array with
   * {@link #value(byte[], int)}. The value is however much is written; the array behind it grows as needed, and
   * the builder reuses it for later requests. (The same writer each time.)
   *
   * @throws IllegalStateException if the value was already given with {@code value(...)}; likewise,
   * {@code value(...)} throws once this was called.
   */
  SectionWriter valueWriter();

  /**
   * Returns the writer of the framing extras. (The same writer each time.)
   */
  SectionWriter framingExtras();

  /**
   * Returns the writer of the extras. (The same writer each time.)
   */
  SectionWriter extras();

  /**
   * Appends to one section of the request: its framing extras, its extras, or its value. Each method returns this writer.
   * <p>
   * {@code putByte} and {@code putShort} throw {@link IllegalArgumentException} if the value doesn't fit
   * (see {@link KeyValueRequestBuilder}). Every method throws {@link IllegalStateException} if no request
   * is being built, or the request's head is complete.
   */
  interface SectionWriter {
    SectionWriter putByte(int b);

    SectionWriter putShort(int s);

    SectionWriter putInt(int i);

    SectionWriter putLong(long l);

    SectionWriter putBytes(byte[] bytes);
  }
}

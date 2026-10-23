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
import org.jspecify.annotations.NullMarked;

import java.nio.ByteBuffer;

/**
 * Builds one KV request packet (see {@link KeyValueRequestEncoder#beginRequest}).
 * <p>
 * Set the header fields the request needs (each defaults to 0), then write its sections, in protocol order:
 * {@link #framingExtras}, {@link #extras}, {@link #key}, and {@link #value}. Each is optional. A section's length
 * is however much the request writes before starting the next section (or finishing), so the request never
 * states a length; the header is filled in when the request is finished.
 * <p>
 * For example:
 * <pre>
 * KeyValueRequestBuilder request = encoder.beginRequest(Opcode.SET, opaque).partition(partition());
 * putMutationFlexibleExtras(request.framingExtras(), ...);
 * request.extras().putInt(flags).putInt(expiry);
 * putKeyWithCollection(request.key(), collection);
 * request.value(content, content.length);
 * </pre>
 * Each section method returns the same buffer, positioned where the section starts. It holds at most
 * {@link KeyValueRequestEncoder#MAX_REQUEST_HEAD_SIZE} bytes of header, framing extras, extras and key.
 * <p>
 * The request is flexible ("alternative" framing) if it has framing extras.
 */
@Stability.Internal
@NullMarked
public interface KeyValueRequestBuilder {
  KeyValueRequestBuilder datatype(byte datatype);

  KeyValueRequestBuilder partition(short partition);

  KeyValueRequestBuilder cas(long cas);

  /**
   * Starts the framing extras, and returns the buffer to write them into.
   *
   * @throws IllegalStateException if a later section was already started.
   */
  ByteBuffer framingExtras();

  /**
   * Starts the extras, and returns the buffer to write them into.
   *
   * @throws IllegalStateException if a later section was already started.
   */
  ByteBuffer extras();

  /**
   * Starts the key, and returns the buffer to write it into.
   *
   * @throws IllegalStateException if the value was already given.
   */
  ByteBuffer key();

  /**
   * Sets the value (body), which is written when the request is finished. Ends the request: nothing more may be
   * written into the buffer.
   *
   * @param value must not change until the request is finished.
   * @param length how much of {@code value} (from the start) is the value.
   * @throws IllegalStateException if the value was already given.
   */
  void value(byte[] value, int length);
}

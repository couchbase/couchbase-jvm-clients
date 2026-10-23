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

/**
 * The stages of building one KV request packet (see {@link KeyValueRequestEncoder#beginRequest},
 * which returns the first, {@link HeaderStage}).
 * <p>
 * Set the header fields the request needs (each defaults to 0), then write its sections, in protocol order:
 * {@link HeaderStage#startFramingExtras}, {@link HeaderStage#startExtras}, {@link HeaderStage#startKey},
 * and {@link HeaderStage#value}. Each is optional. A section's length is however much the request writes into it,
 * so the request never states a length; the header is filled in when the request is finished. A section that's
 * started but left empty is the same as one that's skipped.
 * <p>
 * Each step returns the next stage, which offers only what may follow, so a request is one chain:
 * <pre>
 * encoder.beginRequest(Opcode.SET, opaque).partition(partition())
 *   .startExtras().putInt(flags).putInt(expiry)
 *   .startKey().putBytes(collection).putBytes(key)
 *   .value(content);
 * </pre>
 * Keep using the stage the last step returned. A stage that was left behind is caught at run time, not compile
 * time: writing to an earlier section, or starting a section out of order, throws {@link IllegalStateException}.
 * (The stages are created once per encoder and reused, so building allocates nothing. So a stage kept from an
 * earlier request isn't caught if the current request happens to be writing the same section.)
 * <p>
 * {@code putByte} and {@code putShort} take an int, for convenience, and accept a signed or unsigned value
 * (-128 to 255, and -32768 to 65535); anything else is rejected, rather than truncated.
 * <p>
 * The header, framing extras, extras and key may take at most {@link KeyValueRequestEncoder#MAX_REQUEST_HEAD_SIZE}
 * bytes. The request is flexible ("alternative" framing) if it has framing extras.
 */
@Stability.Internal
@NullMarked
public final class KeyValueRequestBuilder {
  private KeyValueRequestBuilder() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Sets the header fields; then any section, or the value, may follow.
   */
  public interface HeaderStage {
    /**
     * Sets the datatype bits; -128 to 255, so a signed or unsigned byte.
     *
     * @throws IllegalArgumentException if it's out of that range.
     */
    HeaderStage datatype(int datatype);

    /**
     * Sets the partition (vbucket); 0 to 65535.
     *
     * @throws IllegalArgumentException if it's out of that range.
     */
    HeaderStage partition(int partition);

    HeaderStage cas(long cas);

    /**
     * Starts the framing extras.
     */
    FramingExtrasStage startFramingExtras();

    /**
     * Starts the extras.
     */
    ExtrasStage startExtras();

    /**
     * Starts the key.
     */
    KeyStage startKey();

    /**
     * Gives the value (body): the last section, so nothing can be written after it. The array isn't copied now;
     * it's written when the caller finishes the request, so it must not change until then.
     *
     * @param length how much of {@code value} (from the start) is the value.
     */
    void value(byte[] value, int length);

    /**
     * Gives all of {@code value} as the value. See {@link #value(byte[], int)}.
     */
    void value(byte[] value);
  }

  /**
   * Writes the framing extras; then the extras, key or value may follow.
   */
  public interface FramingExtrasStage {
    FramingExtrasStage putByte(int b);

    FramingExtrasStage putShort(int s);

    FramingExtrasStage putInt(int i);

    FramingExtrasStage putBytes(byte[] bytes);

    ExtrasStage startExtras();

    KeyStage startKey();

    /**
     * See {@link HeaderStage#value(byte[], int)}.
     */
    void value(byte[] value, int length);

    /**
     * See {@link HeaderStage#value(byte[])}.
     */
    void value(byte[] value);
  }

  /**
   * Writes the extras; then the key or value may follow.
   */
  public interface ExtrasStage {
    ExtrasStage putByte(int b);

    ExtrasStage putShort(int s);

    ExtrasStage putInt(int i);

    ExtrasStage putLong(long l);

    ExtrasStage putBytes(byte[] bytes);

    KeyStage startKey();

    /**
     * See {@link HeaderStage#value(byte[], int)}.
     */
    void value(byte[] value, int length);

    /**
     * See {@link HeaderStage#value(byte[])}.
     */
    void value(byte[] value);
  }

  /**
   * Writes the key; then the value may follow.
   */
  public interface KeyStage {
    KeyStage putBytes(byte[] bytes);

    /**
     * See {@link HeaderStage#value(byte[], int)}.
     */
    void value(byte[] value, int length);

    /**
     * See {@link HeaderStage#value(byte[])}.
     */
    void value(byte[] value);
  }
}

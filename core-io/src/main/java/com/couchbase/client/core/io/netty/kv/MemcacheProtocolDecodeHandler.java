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

package com.couchbase.client.core.io.netty.kv;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * The {@link MemcacheProtocolDecodeHandler} is a lightweight decoder that understands the
 * KV header and aggregates header and value into one buffer before passing it on.
 *
 * @since 2.0.0
 */
public class MemcacheProtocolDecodeHandler extends LengthFieldBasedFrameDecoder {

  /**
   * Approximates the maximum frame length. Technically it can be longer
   * than Integer.MAX_VALUE in the protocol itself, but we have a 20MB
   * document limit right now and the max value permits up into the
   * GB range which we are likely not to hit.
   *
   * <p>If you read this because we've hit it - good luck figuring out
   * a solution with long values.</p>
   */
  private static final int MAX_FRAME_LENGTH = Integer.MAX_VALUE;

  /**
   * The offset to the "total body length" field in the protocol
   * header.
   */
  private static final int LENGTH_FIELD_OFFSET = 8;

  /**
   * The length of the "total body length" field in the protocol
   * header.
   */
  private static final int LENGTH_FIELD_LENGTH = 4;

  /**
   * Both the opaque and the cas value follow the "total body
   * length" field, so we need to tell the frame decoder to
   * adjust for those two fields to find the end of the header.
   */
  private static final int LENGTH_ADJUSTMENT = 12;

  /**
   * We don't need to strip any additional header bytes since
   * we want the full header up in the next handlers.
   */
  private static final int INITIAL_BYTES_TO_STRIP = 0;

  public MemcacheProtocolDecodeHandler() {
    super(
      MAX_FRAME_LENGTH,
      LENGTH_FIELD_OFFSET,
      LENGTH_FIELD_LENGTH,
      LENGTH_ADJUSTMENT,
      INITIAL_BYTES_TO_STRIP
    );
  }

}
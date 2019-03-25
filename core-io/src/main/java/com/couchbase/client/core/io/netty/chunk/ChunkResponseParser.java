/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty.chunk;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.ChannelConfig;
import com.couchbase.client.core.msg.chunk.ChunkHeader;
import com.couchbase.client.core.msg.chunk.ChunkRow;
import com.couchbase.client.core.msg.chunk.ChunkTrailer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

/**
 * Defines the common parser interface for all chunked response parser implementations.
 */
public interface ChunkResponseParser<H extends ChunkHeader, ROW extends ChunkRow,
  T extends ChunkTrailer> {

  /**
   * Once the header is completely available, returns a non-absent value of it.
   *
   * <p>It is important to provide a non-absent value even if some parts are optional because
   * the related IO components will only proceed if a header is available eventually.</p>
   */
  Optional<H> header();

  /**
   * If the parser sees an error, it should fill this optional so that if the IO
   * layer needs to fail the topmost future it will be passed in.
   */
  Optional<Throwable> error();

  /**
   * Called when a fresh request should be initialized.
   *
   * <p>Also use this to reset any earlier state.</p>
   *
   * @param content the content slice to use on parsing.
   * @param channelConfig the channel config used for backpressure auto-read.
   */
  void initialize(final ByteBuf content, ChannelConfig channelConfig);

  /**
   * Receives a signal complete, usually to complete the rows and the trailer.
   */
  void signalComplete();

  /**
   * Signal to parse the content to proceed.
   *
   * <p>If true is returned, the read bits from the content buffer are going to be cleaned up.</p>
   */
  boolean parse();

  /**
   * Returns the currently assigned flux for the rows.
   */
  Flux<ROW> rows();

  /**
   * Returns the currently assigned mono for the trailer bits.
   */
  Mono<T> trailer();

}

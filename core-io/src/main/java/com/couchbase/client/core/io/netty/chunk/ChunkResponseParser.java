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
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.RequestContext;
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
   *
   * @param lastChunk if we are currently parsing the last chunk.
   */
  Optional<H> header(boolean lastChunk);

  /**
   * If the parser sees an error, it should fill this optional so that if the IO
   * layer needs to fail the topmost future it will be passed in.
   */
  Optional<CouchbaseException> error();

  /**
   * If the parser fails due to malformed input the cause is returned here.
   */
  Optional<CouchbaseException> decodingFailure();

  /**
   * Begins a new parsing session.
   *
   * @param channelConfig the channel config used for backpressure auto-read.
   */
  void initialize(ChannelConfig channelConfig);

  /**
   * Releases resources managed by the parser and prepares it for reuse.
   */
  void cleanup();

  /**
   * Parses the given JSON document fragment. The parser takes ownership of the buffer
   * and is responsible for releasing it.
   */
  void feed(ByteBuf input);

  /**
   * Indicates the complete JSON document has been fed to the parser.
   */
  void endOfInput();

  /**
   * Returns the currently assigned flux for the rows.
   */
  Flux<ROW> rows();

  /**
   * Returns the currently assigned mono for the trailer bits.
   */
  Mono<T> trailer();

  /**
   * Sets the request context for the current request in the parser, can be used for error handling.
   */
  void updateRequestContext(RequestContext requestContext);

  /**
   * Sets the current response header if present.
   */
  void updateResponseHeader(HttpResponse responseHeader);

}

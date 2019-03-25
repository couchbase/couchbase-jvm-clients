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

package com.couchbase.client.core.msg.chunk;

import com.couchbase.client.core.msg.ResponseStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Marker interface to describe how the chunked response can be decoded.
 */
public interface ChunkDecodable<H extends ChunkHeader, ROW extends  ChunkRow, T extends ChunkTrailer, R extends ChunkedResponse<H, ROW, T>> {

  /**
   * Decodes a chunked response into the response format.
   *
   * @param status the http response status.
   * @param header the chunk header.
   * @param rows the chunk rows.
   * @param trailer the chunk trailer.
   * @return a decoded response including all the chunk parts.
   */
  R decode(ResponseStatus status, H header, Flux<ROW> rows, Mono<T> trailer);

}

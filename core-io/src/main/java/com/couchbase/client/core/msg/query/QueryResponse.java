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

package com.couchbase.client.core.msg.query;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.chunk.ChunkedResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class QueryResponse
  extends BaseResponse
  implements ChunkedResponse<QueryChunkHeader, QueryChunkRow, QueryChunkTrailer> {

  private final QueryChunkHeader header;
  private final Flux<QueryChunkRow> rows;
  private final Mono<QueryChunkTrailer> trailer;

  QueryResponse(final ResponseStatus status, final QueryChunkHeader header,
                final Flux<QueryChunkRow> rows, final Mono<QueryChunkTrailer> trailer) {
    super(status);
    this.header = header;
    this.rows = rows;
    this.trailer = trailer;
  }

  public static RuntimeException errorSignatureNotPresent() {
    return new IllegalStateException("Field 'signature' was not present in response");
  }

  public static RuntimeException errorWarningsNotPresent() {
    return new IllegalStateException("Field 'warnings' was not present in response");
  }

  public static RuntimeException errorProfileNotPresent() {
    return new IllegalStateException("Field 'profile' was not present in response");
  }

  public static RuntimeException errorIncompleteResponse() {
    return new IllegalStateException("Not all expected fields were returned in response");
  }


  @Override
  public QueryChunkHeader header() {
    return header;
  }

  @Override
  public Flux<QueryChunkRow> rows() {
    return rows;
  }

  @Override
  public Mono<QueryChunkTrailer> trailer() {
    return trailer;
  }

  @Override
  public String toString() {
    return "NewQueryResponse{" +
      "header=" + header +
      ", rows=" + rows +
      ", trailer=" + trailer +
      '}';
  }
}

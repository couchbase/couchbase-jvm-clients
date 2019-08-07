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

package com.couchbase.client.core.io.netty.search;

import com.couchbase.client.core.error.SearchServiceException;
import com.couchbase.client.core.io.netty.chunk.BaseChunkResponseParser;
import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.search.SearchChunkHeader;
import com.couchbase.client.core.msg.search.SearchChunkRow;
import com.couchbase.client.core.msg.search.SearchChunkTrailer;

import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SearchChunkResponseParser
  extends BaseChunkResponseParser<SearchChunkHeader, SearchChunkRow, SearchChunkTrailer> {

  private byte[] status;
  private byte[] error;

    private long totalRows;
    private double maxScore;
    private long took;

  @Override
  protected void doCleanup() {
    status = null;
    error = null;
    totalRows = 0;
    maxScore = 0.0;
    took = 0;
  }

  private final JsonStreamParser.Builder parserBuilder = JsonStreamParser.builder()
    .doOnValue("/status", v -> status = v.readBytes())
    .doOnValue("/error", v -> {
      error = v.readBytes();
      failRows(new SearchServiceException(new String(error, UTF_8)));
    })
    .doOnValue("/hits/-", v -> emitRow(new SearchChunkRow(v.readBytes())))
    .doOnValue("/total_rows", v -> totalRows = v.readLong())
    .doOnValue("/max_score", v -> maxScore = v.readDouble())
    .doOnValue("/took", v -> took = v.readLong());

  @Override
  protected JsonStreamParser.Builder parserBuilder() {
    return parserBuilder;
  }

  @Override
  public Optional<SearchChunkHeader> header() {
    return Optional.ofNullable(status).map(SearchChunkHeader::new);
  }

  @Override
  public Optional<Throwable> error() {
    return Optional.ofNullable(error).map(e -> new SearchServiceException(new String(e, UTF_8)));
  }

    @Override
    public void signalComplete() {
        completeRows();
        completeTrailer(new SearchChunkTrailer(totalRows, maxScore, took));
    }
}

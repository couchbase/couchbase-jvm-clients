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

import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.core.error.AuthenticationException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.InternalServerException;
import com.couchbase.client.core.error.SearchErrorContext;
import com.couchbase.client.core.error.SearchIndexNotFoundException;
import com.couchbase.client.core.io.netty.HttpProtocol;
import com.couchbase.client.core.io.netty.chunk.BaseChunkResponseParser;
import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.search.SearchChunkHeader;
import com.couchbase.client.core.msg.search.SearchChunkRow;
import com.couchbase.client.core.msg.search.SearchChunkTrailer;

import java.util.Optional;

public class SearchChunkResponseParser
  extends BaseChunkResponseParser<SearchChunkHeader, SearchChunkRow, SearchChunkTrailer> {

  private byte[] status;
  private byte[] error;
  private byte[] facets;

  private long totalRows;
  private double maxScore;
  private long took;

  @Override
  protected void doCleanup() {
    status = null;
    error = null;
    facets = null;
    totalRows = 0;
    maxScore = 0.0;
    took = 0;
  }

  private final JsonStreamParser.Builder parserBuilder = JsonStreamParser.builder()
    .doOnValue("/status", v -> status = v.readBytes())
    .doOnValue("/error", v -> {
      error = v.readBytes();
      failRows(errorsToThrowable(error));
    })
    .doOnValue("/hits/-", v -> emitRow(new SearchChunkRow(v.readBytes())))
    .doOnValue("/total_rows", v -> totalRows = v.readLong())
    .doOnValue("/max_score", v -> maxScore = v.readDouble())
    .doOnValue("/took", v -> took = v.readLong())
    .doOnValue("/facets", v -> facets = v.readBytes());

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
    return Optional.ofNullable(error).map(this::errorsToThrowable);
  }

  private Throwable errorsToThrowable(final byte[] bytes) {
    int statusCode = responseHeader().status().code();
    String errorDecoded = bytes == null || bytes.length == 0 ? "" : new String(bytes, CharsetUtil.UTF_8);
    SearchErrorContext errorContext = new SearchErrorContext(
      HttpProtocol.decodeStatus(responseHeader().status()),
      requestContext(),
      statusCode
    );
    if (statusCode == 400 && errorDecoded.contains("index not found")) {
      return new SearchIndexNotFoundException(errorContext);
    } else if (statusCode == 500) {
      return new InternalServerException(errorContext);
    } else if (statusCode == 401 || statusCode == 403) {
      return new AuthenticationException("Could not authenticate search query", errorContext, null);
    }
    return new CouchbaseException("Unknown search error: " + errorDecoded, errorContext);
  }

  @Override
  public void signalComplete() {
    completeRows();
    completeTrailer(new SearchChunkTrailer(totalRows, maxScore, took, facets));
  }

}

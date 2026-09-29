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

package com.couchbase.client.core.service;

import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;

import java.io.Closeable;
import java.io.InputStream;
import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class QueryResponseParser implements Closeable {
  private final Consumer<QueryChunkHeader> headerCallback;
  private final Consumer<byte[]> rowCallback;
  private final JsonStreamParser parser = newStreamParser();

  private final MutableQueryMetadata meta = new MutableQueryMetadata();
  private boolean headerCompleted = false;

  public QueryResponseParser(
    Consumer<QueryChunkHeader> headerCallback,
    Consumer<byte[]> rowCallback
  ) {
    this.headerCallback = requireNonNull(headerCallback);
    this.rowCallback = requireNonNull(rowCallback);
  }

  @Override
  public void close() {
    parser.close();
  }

  public static QueryChunkTrailer parse(
    InputStream is,
    Consumer<QueryChunkHeader> headerCallback,
    Consumer<byte[]> rowCallback
  ) {
    try (QueryResponseParser p = new QueryResponseParser(headerCallback, rowCallback)) {
      p.parser.feed(is);
      p.parser.endOfInput();

      return p.trailer();
    }
  }

  private JsonStreamParser newStreamParser() {
    JsonStreamParser.Builder parserBuilder = JsonStreamParser.builder()
      .doOnValue("/requestID", v -> meta.requestId = v.readString())
      .doOnValue("/signature", v -> meta.signature = v.bytes())
      .doOnValue("/clientContextID", v -> meta.clientContextId = v.readString())
      .doOnValue("/prepared", v -> meta.prepared = v.readString())
      .doOnValue("/results/-", v -> {
        maybeCompleteHeader();
        rowCallback.accept(v.bytes());
      })
      .doOnValue("/status", v -> {
        maybeCompleteHeader();
        meta.status = v.readString();
      })
      .doOnValue("/metrics", v -> meta.metrics = v.bytes())
      .doOnValue("/profile", v -> meta.profile = v.bytes())
      .doOnValue("/errors", v -> meta.errors = v.bytes())
      .doOnValue("/warnings", v -> meta.warnings = v.bytes());

    return parserBuilder.build();
  }

  private void maybeCompleteHeader() {
    if (headerCompleted) return;

    headerCompleted = true;

    QueryChunkHeader header = new QueryChunkHeader(
      meta.requestId,
      Optional.ofNullable(meta.clientContextId),
      Optional.ofNullable(meta.signature),
      Optional.ofNullable(meta.prepared)
    );

    headerCallback.accept(header);
  }

  private QueryChunkTrailer trailer() {
    return new QueryChunkTrailer(
      meta.status,
      Optional.ofNullable(meta.metrics),
      Optional.ofNullable(meta.warnings),
      Optional.ofNullable(meta.errors),
      Optional.ofNullable(meta.profile)
    );
  }
}

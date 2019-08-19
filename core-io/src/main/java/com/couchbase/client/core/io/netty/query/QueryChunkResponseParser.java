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

package com.couchbase.client.core.io.netty.query;

import com.couchbase.client.core.error.QueryException;
import com.couchbase.client.core.io.netty.chunk.BaseChunkResponseParser;
import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;

import java.util.Optional;

public class QueryChunkResponseParser
  extends BaseChunkResponseParser<QueryChunkHeader, QueryChunkRow, QueryChunkTrailer> {

  private String requestId;
  private Optional<byte[]> signature;
  private Optional<String> clientContextId;
  private Optional<String> prepared;

  private String status;
  private byte[] metrics;
  private byte[] warnings;
  private byte[] errors;
  private byte[] profile;

  @Override
  protected void doCleanup() {
    requestId = null;
    signature = Optional.empty();
    clientContextId = Optional.empty();
    prepared = Optional.empty();

    status = null;
    metrics = null;
    warnings = null;
    errors = null;
    profile = null;
  }

  private final JsonStreamParser.Builder parserBuilder = JsonStreamParser.builder()
    .doOnValue("/requestID", v -> requestId = v.readString())
    .doOnValue("/signature", v -> signature = Optional.of(v.readBytes()))
    .doOnValue("/clientContextID", v -> clientContextId = Optional.of(v.readString()))
    .doOnValue("/prepared", v -> prepared = Optional.of(v.readString()))
    .doOnValue("/results/-", v -> {
      markHeaderComplete();
      emitRow(new QueryChunkRow(v.readBytes()));
    })
    .doOnValue("/status", v -> {
      markHeaderComplete();
      status = v.readString();
    })
    .doOnValue("/metrics", v -> metrics = v.readBytes())
    .doOnValue("/profile", v -> profile = v.readBytes())
    .doOnValue("/errors", v -> {
      errors = v.readBytes();
      failRows(new QueryException(errors));
    })
    .doOnValue("/warnings", v -> warnings = v.readBytes());

  @Override
  protected JsonStreamParser.Builder parserBuilder() {
    return parserBuilder;
  }

  @Override
  public Optional<QueryChunkHeader> header() {
    return isHeaderComplete()
      ? Optional.of(new QueryChunkHeader(requestId, clientContextId, signature, prepared))
      : Optional.empty();
  }

  @Override
  public Optional<Throwable> error() {
    return Optional.ofNullable(errors).map(QueryException::new);
  }

  @Override
  public void signalComplete() {
    completeRows();
    completeTrailer(new QueryChunkTrailer(
      status,
      Optional.ofNullable(metrics),
      Optional.ofNullable(warnings),
      Optional.ofNullable(errors),
      Optional.ofNullable(profile)
    ));
  }

}

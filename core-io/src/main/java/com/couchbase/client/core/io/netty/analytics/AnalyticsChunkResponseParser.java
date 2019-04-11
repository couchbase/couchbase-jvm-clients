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

package com.couchbase.client.core.io.netty.analytics;

import com.couchbase.client.core.error.AnalyticsServiceException;
import com.couchbase.client.core.io.netty.chunk.BaseChunkResponseParser;
import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkHeader;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkRow;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkTrailer;

import java.util.Optional;

public class AnalyticsChunkResponseParser
  extends BaseChunkResponseParser<AnalyticsChunkHeader, AnalyticsChunkRow, AnalyticsChunkTrailer> {

  private String requestId;
  private Optional<byte[]> signature;
  private Optional<String> clientContextId;

  private String status;
  private byte[] metrics;
  private byte[] warnings;
  private byte[] errors;

  @Override
  protected void doCleanup() {
    requestId = null;
    signature = Optional.empty();
    clientContextId = Optional.empty();

    status = null;
    metrics = null;
    warnings = null;
    errors = null;
  }

  private final JsonStreamParser.Builder parserBuilder = JsonStreamParser.builder()
    .doOnValue("/requestID", v -> requestId = v.readString())
    .doOnValue("/signature", v -> signature = Optional.of(v.readBytes()))
    .doOnValue("/clientContextID", v -> clientContextId = Optional.of(v.readString()))
    .doOnValue("/results/-", v -> {
      markHeaderComplete();
      emitRow(new AnalyticsChunkRow(v.readBytes()));
    })
    .doOnValue("/status", v -> {
      markHeaderComplete();
      status = v.readString();
    })
    .doOnValue("/metrics", v -> metrics = v.readBytes())
    .doOnValue("/errors", v -> {
      errors = v.readBytes();
      failRows(new AnalyticsServiceException(errors));
    })
    .doOnValue("/warnings", v -> warnings = v.readBytes());

  @Override
  protected JsonStreamParser.Builder parserBuilder() {
    return parserBuilder;
  }

  @Override
  public Optional<AnalyticsChunkHeader> header() {
    return isHeaderComplete()
      ? Optional.of(new AnalyticsChunkHeader(requestId, clientContextId, signature))
      : Optional.empty();
  }

  @Override
  public Optional<Throwable> error() {
    return Optional.ofNullable(errors).map(AnalyticsServiceException::new);
  }

  @Override
  public void signalComplete() {
    completeRows();
    completeTrailer(new AnalyticsChunkTrailer(
      status,
      metrics,
      Optional.ofNullable(warnings),
      Optional.ofNullable(errors)
    ));
  }

}

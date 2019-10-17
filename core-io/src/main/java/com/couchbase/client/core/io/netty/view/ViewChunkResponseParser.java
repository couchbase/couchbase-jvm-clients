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

package com.couchbase.client.core.io.netty.view;

import com.couchbase.client.core.error.ViewServiceException;
import com.couchbase.client.core.io.netty.chunk.BaseChunkResponseParser;
import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.view.ViewChunkHeader;
import com.couchbase.client.core.msg.view.ViewChunkRow;
import com.couchbase.client.core.msg.view.ViewChunkTrailer;
import com.couchbase.client.core.msg.view.ViewError;

import java.util.Optional;

public class ViewChunkResponseParser
  extends BaseChunkResponseParser<ViewChunkHeader, ViewChunkRow, ViewChunkTrailer> {

  private Long totalRows;

  private Optional<byte[]> debug;
  private Optional<ViewError> error;

  private final JsonStreamParser.Builder parserBuilder = JsonStreamParser.builder()
    .doOnValue("/total_rows", v -> totalRows = v.readLong())
    .doOnValue("/rows/-", v -> {
      emitRow(new ViewChunkRow(v.readBytes()));
    })
    .doOnValue("/debug_info", v -> debug = Optional.of(v.readBytes()))
    .doOnValue("/error", v -> {
      String data = v.readString();

      ViewError current = error.orElse(new ViewError(null, null));
      error = Optional.of(new ViewError(data, current.reason()));
    })
    .doOnValue("/reason", v -> {
      String data = v.readString();

      ViewError current = error.orElse(new ViewError(null, null));
      error = Optional.of(new ViewError(current.error(), data));
    });

  @Override
  protected JsonStreamParser.Builder parserBuilder() {
    return parserBuilder;
  }

  @Override
  protected void doCleanup() {
    totalRows = null;
    debug = Optional.empty();
    error = Optional.empty();
  }

  @Override
  public Optional<ViewChunkHeader> header() {
    return (totalRows != null)
      ? Optional.of(new ViewChunkHeader(totalRows, debug))
      : Optional.empty();
  }

  @Override
  public Optional<Throwable> error() {
    return error.map(e -> new ViewServiceException(e.reassemble()));
  }

  @Override
  public void signalComplete() {
    Optional<Throwable> maybeError = error();
    if (maybeError.isPresent()) {
      failRows(maybeError.get());
    } else {
      completeRows();
    }
    completeTrailer(new ViewChunkTrailer(error));
  }

}

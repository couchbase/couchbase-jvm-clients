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
import com.couchbase.client.core.msg.view.ViewChunkHeader;
import com.couchbase.client.core.msg.view.ViewChunkRow;
import com.couchbase.client.core.msg.view.ViewChunkTrailer;
import com.couchbase.client.core.msg.view.ViewError;
import com.couchbase.client.core.util.yasjl.ByteBufJsonParser;
import com.couchbase.client.core.util.yasjl.Callbacks.JsonPointerCB1;
import com.couchbase.client.core.util.yasjl.JsonPointer;

import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ViewChunkResponseParser
  extends BaseChunkResponseParser<ViewChunkHeader, ViewChunkRow, ViewChunkTrailer> {

  private Long totalRows;
  private Optional<byte[]> debug;

  private Optional<ViewError> error;

  @Override
  protected ByteBufJsonParser initParser() {
    return new ByteBufJsonParser(new JsonPointer[] {
      new JsonPointer("/total_rows", (JsonPointerCB1) value -> {
        totalRows = Long.parseLong(value.toString(UTF_8));
      }),
      new JsonPointer("/rows/-", (JsonPointerCB1) value -> {
        if (debug == null) {
          debug = Optional.empty();
        }
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        emitRow(new ViewChunkRow(data));
      }),
      new JsonPointer("/debug_info", (JsonPointerCB1) value -> {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        debug = Optional.of(data);
      }),
      new JsonPointer("/error", (JsonPointerCB1) value -> {
        String data = value.toString(UTF_8);
        data = data.substring(1, data.length() - 1);
        value.release();

        ViewError current = error.orElse(new ViewError(null, null));
        error = Optional.of(new ViewError(data, current.reason()));
      }),
      new JsonPointer("/reason", (JsonPointerCB1) value -> {
        String data = value.toString(UTF_8);
        data = data.substring(1, data.length() - 1);
        value.release();

        ViewError current = error.orElse(new ViewError(null, null));
        error = Optional.of(new ViewError(current.error(), data));
      })
    });
  }

  @Override
  protected void resetState() {
    totalRows = 0L;
    error = Optional.empty();
    debug = Optional.empty();
  }

  @Override
  public Optional<ViewChunkHeader> header() {
    if (totalRows != null && debug != null) {
      return Optional.of(new ViewChunkHeader(totalRows, debug));
    }
    return Optional.empty();
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

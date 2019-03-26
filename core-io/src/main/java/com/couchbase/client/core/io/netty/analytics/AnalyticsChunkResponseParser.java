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
import com.couchbase.client.core.error.QueryServiceException;
import com.couchbase.client.core.io.netty.chunk.BaseChunkResponseParser;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkHeader;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkRow;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkTrailer;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.core.util.yasjl.ByteBufJsonParser;
import com.couchbase.client.core.util.yasjl.Callbacks.JsonPointerCB1;
import com.couchbase.client.core.util.yasjl.JsonPointer;

import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AnalyticsChunkResponseParser
  extends BaseChunkResponseParser<AnalyticsChunkHeader, AnalyticsChunkRow, AnalyticsChunkTrailer> {

  private String requestId;
  private Optional<byte[]> signature;
  private Optional<String> clientContextId;
  private String status;
  private byte[] metrics;
  private byte[] warnings;
  private byte[] errors;
  private byte[] profile;

  AnalyticsChunkResponseParser() {
  }

  @Override
  protected void resetState() {
    requestId = null;
    signature = null;
    clientContextId = null;
    status = null;
    metrics = null;
    warnings = null;
    errors = null;
    profile = null;
  }

  @Override
  protected ByteBufJsonParser initParser() {
    return new ByteBufJsonParser(new JsonPointer[] {
      new JsonPointer("/requestID", (JsonPointerCB1) value -> {
        String data = value.toString(UTF_8);
        data = data.substring(1, data.length() - 1);
        value.release();
        requestId = data;
      }),
      new JsonPointer("/signature", (JsonPointerCB1) value -> {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        signature = Optional.of(data);
      }),
      new JsonPointer("/clientContextID", (JsonPointerCB1) value -> {
        String data = value.toString(UTF_8);
        data = data.substring(1, data.length() - 1);
        value.release();
        clientContextId = Optional.of(data);
      }),
      new JsonPointer("/results/-", (JsonPointerCB1) value -> {
        if (clientContextId == null) {
          clientContextId = Optional.empty();
        }
        if (signature == null) {
          signature = Optional.empty();
        }

        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        emitRow(new AnalyticsChunkRow(data));
      }),
      new JsonPointer("/status", (JsonPointerCB1) value -> {
        if (clientContextId == null) {
          clientContextId = Optional.empty();
        }
        if (signature == null) {
          signature = Optional.empty();
        }

        String data = value.toString(UTF_8);
        data = data.substring(1, data.length() - 1);
        value.release();
        status = data;
      }),
      new JsonPointer("/metrics", (JsonPointerCB1) value -> {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        metrics = data;
      }),
      new JsonPointer("/profile", (JsonPointerCB1) value -> {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        profile = data;
      }),
      new JsonPointer("/errors", (JsonPointerCB1) value -> {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        errors = data;
        failRows(new AnalyticsServiceException(errors));
      }),
      new JsonPointer("/warnings", (JsonPointerCB1) value -> {
        byte[] data = new byte[value.readableBytes()];
        value.readBytes(data);
        value.release();
        warnings = data;
      })
    });
  }

  @Override
  public Optional<AnalyticsChunkHeader> header() {
    if (requestId != null && signature != null && clientContextId != null) {
      return Optional.of(new AnalyticsChunkHeader(requestId, clientContextId, signature));
    }
    return Optional.empty();
  }

  @Override
  public Optional<Throwable> error() {
    if (errors == null) {
      return Optional.empty();
    } else {
      return Optional.of(new AnalyticsServiceException(errors));
    }
  }

  @Override
  public void signalComplete() {
    completeRows();
    completeTrailer(new AnalyticsChunkTrailer(
      status,
      Optional.ofNullable(metrics),
      Optional.ofNullable(warnings),
      Optional.ofNullable(errors),
      Optional.ofNullable(profile)
    ));
  }

}

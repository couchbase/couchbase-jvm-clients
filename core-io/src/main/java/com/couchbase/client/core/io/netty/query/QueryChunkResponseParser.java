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

import com.couchbase.client.core.error.QueryServiceException;
import com.couchbase.client.core.io.netty.chunk.BaseChunkResponseParser;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.core.util.yasjl.ByteBufJsonParser;
import com.couchbase.client.core.util.yasjl.JsonPointer;

import java.util.Optional;

public class QueryChunkResponseParser
  extends BaseChunkResponseParser<QueryChunkHeader, QueryChunkRow, QueryChunkTrailer> {

  private String requestId;
  private Optional<byte[]> signature;
  private Optional<String> clientContextId;
  private String status;
  private byte[] metrics;
  private byte[] warnings;
  private byte[] errors;
  private byte[] profile;

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
      new JsonPointer("/requestID", value -> requestId = Mapper.decodeInto(value, String.class)),
      new JsonPointer("/signature", value -> signature = Optional.of(value)),
      new JsonPointer("/clientContextID", value -> clientContextId = Optional.of(Mapper.decodeInto(value, String.class))),
      new JsonPointer("/results/-", value -> {
        if (clientContextId == null) {
          clientContextId = Optional.empty();
        }
        if (signature == null) {
          signature = Optional.empty();
        }

        emitRow(new QueryChunkRow(value));
      }),
      new JsonPointer("/status", value -> {
        if (clientContextId == null) {
          clientContextId = Optional.empty();
        }
        if (signature == null) {
          signature = Optional.empty();
        }

        status = Mapper.decodeInto(value, String.class);
      }),
      new JsonPointer("/metrics", value -> metrics = value),
      new JsonPointer("/profile", value -> profile = value),
      new JsonPointer("/errors", value -> {
        errors = value;
        failRows(new QueryServiceException(errors));
      }),
      new JsonPointer("/warnings", value -> warnings = value)
    });
  }

  @Override
  public Optional<QueryChunkHeader> header() {
    if (requestId != null && signature != null && clientContextId != null) {
      return Optional.of(new QueryChunkHeader(requestId, clientContextId, signature));
    }
    return Optional.empty();
  }

  @Override
  public Optional<Throwable> error() {
    if (errors == null) {
      return Optional.empty();
    } else {
      return Optional.of(new QueryServiceException(errors));
    }
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

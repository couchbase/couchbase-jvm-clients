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

import com.couchbase.client.core.error.AuthenticationException;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.core.error.InternalServerException;
import com.couchbase.client.core.error.ParsingFailedException;
import com.couchbase.client.core.error.PlanningFailedException;
import com.couchbase.client.core.error.PreparedStatementException;
import com.couchbase.client.core.error.QueryErrorContext;
import com.couchbase.client.core.error.QueryIndexException;
import com.couchbase.client.core.io.netty.chunk.BaseChunkResponseParser;
import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class QueryChunkResponseParser
  extends BaseChunkResponseParser<QueryChunkHeader, QueryChunkRow, QueryChunkTrailer> {

  /**
   * Holds error codes that are special cased for prepared statements.
   */
  private static final List<Integer> PREPARED_ERROR_CODES = Arrays.asList(4040, 4050, 4060, 4070, 4080, 4090);

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
      failRows(errorsToThrowable(errors));
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
    return Optional.ofNullable(errors).map(this::errorsToThrowable);
  }

  private Throwable errorsToThrowable(final byte[] bytes) {
    final List<ErrorCodeAndMessage> errors = bytes.length == 0
      ? Collections.emptyList()
      : ErrorCodeAndMessage.fromJsonArray(bytes);
    QueryErrorContext errorContext = new QueryErrorContext(requestContext(), errors);
    if (errors.size() >= 1) {
      int code = errors.get(0).code();
      if (code == 3000) {
        return new ParsingFailedException(errorContext);
      } else if (PREPARED_ERROR_CODES.contains(code)) {
        return new PreparedStatementException(errorContext);
      } else if (code >= 4000 && code < 5000) {
        return new PlanningFailedException(errorContext);
      } else if (code >= 5000 && code < 6000) {
        return new InternalServerException(errorContext);
      } else if (code == 12009) {
        return new CasMismatchException(errorContext);
      } else if (code >= 10000 && code < 11000) {
        return new AuthenticationException("Could not authenticate query", errorContext, null);
      } else if ((code >= 12000 && code < 13000) || (code >= 14000 && code < 15000)) {
        return new QueryIndexException(errorContext);
      }
    }
    return new CouchbaseException("Unknown query error", errorContext);
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

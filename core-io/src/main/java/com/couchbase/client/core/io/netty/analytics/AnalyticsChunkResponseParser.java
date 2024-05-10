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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.CompilationFailureException;
import com.couchbase.client.core.error.CoreErrorCodeAndMessageException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DatasetExistsException;
import com.couchbase.client.core.error.DatasetNotFoundException;
import com.couchbase.client.core.error.DataverseExistsException;
import com.couchbase.client.core.error.DataverseNotFoundException;
import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.InternalServerFailureException;
import com.couchbase.client.core.error.JobQueueFullException;
import com.couchbase.client.core.error.LinkExistsException;
import com.couchbase.client.core.error.LinkNotFoundException;
import com.couchbase.client.core.error.ParsingFailureException;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.core.error.context.AnalyticsErrorContext;
import com.couchbase.client.core.io.netty.chunk.BaseChunkResponseParser;
import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkHeader;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkRow;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkTrailer;
import com.couchbase.client.core.msg.analytics.AnalyticsRequest;

import java.util.Collections;
import java.util.List;
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
  private byte[] plans;

  @Override
  protected void doCleanup() {
    requestId = null;
    signature = Optional.empty();
    clientContextId = Optional.empty();

    status = null;
    metrics = null;
    warnings = null;
    errors = null;
    plans = null;
  }

  private final JsonStreamParser.Builder parserBuilder = JsonStreamParser.builder()
    .doOnValue("/requestID", v -> requestId = v.readString())
    .doOnValue("/signature", v -> signature = Optional.of(v.readBytes()))
    .doOnValue("/plans", v -> plans = v.readBytes())
    .doOnValue("/clientContextID", v -> clientContextId = Optional.of(v.readString()))
    .doOnValue("/results/-", v -> {
      markHeaderComplete();
      emitRow(new AnalyticsChunkRow(v.readBytes()));
    })
    .doOnValue("/status", v -> {
      markHeaderComplete();
      status = v.readString();
    })
    .doOnValue("/metrics", v -> {
      markHeaderComplete();
      metrics = v.readBytes();
    })
    .doOnValue("/errors", v -> {
      errors = v.readBytes();
      failRows(errorsToThrowable(errors, requestContext(), responseHeader().status()));
    })
    .doOnValue("/warnings", v -> warnings = v.readBytes());

  @Override
  protected JsonStreamParser.Builder parserBuilder() {
    return parserBuilder;
  }

  @Override
  public Optional<AnalyticsChunkHeader> header(boolean lastChunk) {
    return isHeaderComplete()
      ? Optional.of(new AnalyticsChunkHeader(requestId, clientContextId, signature))
      : Optional.empty();
  }

  @Override
  public Optional<CouchbaseException> error() {
    return Optional.ofNullable(errors).map(e -> errorsToThrowable(e, requestContext(), responseHeader().status()));
  }

  @Stability.Internal
  static CouchbaseException errorsToThrowable(final byte[] bytes, RequestContext ctx, HttpResponseStatus httpStatus) {
    int httpCode = httpStatus != null ? httpStatus.code() : 0;

    final List<ErrorCodeAndMessage> errors = bytes.length == 0
      ? Collections.emptyList()
      : ErrorCodeAndMessage.from(bytes);
    AnalyticsErrorContext errorContext = new AnalyticsErrorContext(ctx, errors, httpCode);
    if (errors.size() >= 1) {
      ErrorCodeAndMessage error = errors.get(0);

      if (ctx.request() instanceof AnalyticsRequest && !((AnalyticsRequest) ctx.request()).translateExceptions()) {
        // Let Columnar do its own exception translation
        return new CoreErrorCodeAndMessageException(error, errorContext);
      }

      // Analytics error code reference:
      //   https://docs.couchbase.com/server/current/analytics/error-codes.html
      int code = error.code();
      if (code >= 25000 && code < 26000) {
        return new InternalServerFailureException(errorContext);
      } else if (code >= 20000 && code < 21000) {
        return new AuthenticationFailureException("Could not authenticate analytics query", errorContext, null);
      } else if (code == 23000 || code == 23003) {
        return new TemporaryFailureException(errorContext);
      } else if (code == 23007) {
        return new JobQueueFullException(errorContext);
      } else if (code == 24000) {
        return new ParsingFailureException(errorContext);
      } else if (code == 24006) {
        return new LinkNotFoundException(errorContext);
      } else if (code == 24055) {
        return new LinkExistsException(errorContext);
      } else if (code == 24040) {
        return new DatasetExistsException(errorContext);
      } else if (code == 24044 || code == 24045 || code == 24025) {
        return new DatasetNotFoundException(errorContext);
      } else if (code == 24034) {
        return new DataverseNotFoundException(errorContext);
      } else if (code == 24039) {
        return new DataverseExistsException(errorContext);
      } else if (code == 24047) {
        return new IndexNotFoundException(errorContext);
      } else if (code == 24048) {
        return new IndexExistsException(errorContext);
      } else if (code > 24000 && code < 25000) {
        return new CompilationFailureException(errorContext);
      } else {
        return new CouchbaseException("Unknown analytics error: " + error, errorContext);
      }
    }
    return new CouchbaseException("Unknown analytics error", errorContext);
  }

  @Override
  public void signalComplete() {
    completeRows();
    completeTrailer(new AnalyticsChunkTrailer(
      status,
      metrics,
      Optional.ofNullable(warnings),
      Optional.ofNullable(errors),
      Optional.ofNullable(plans)
    ));
  }

}

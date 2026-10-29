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

import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DmlFailureException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.InternalServerFailureException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.ParsingFailureException;
import com.couchbase.client.core.error.PlanningFailureException;
import com.couchbase.client.core.error.PreparedStatementFailureException;
import com.couchbase.client.core.error.QuotaLimitedException;
import com.couchbase.client.core.error.RateLimitedException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.context.QueryErrorContext;
import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryReason;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static java.util.Objects.requireNonNull;

/**
 * The query service's streamed JSON response, for {@link StreamingJsonResponseHandler}.
 * <p>
 * The header is complete when the first row arrives, or the status if there are no rows.
 */
@NullMarked
final class QueryResponseFormat
  implements StreamingResponseFormat<QueryChunkHeader, QueryChunkRow, QueryChunkTrailer, QueryResponse> {

  static final QueryResponseFormat INSTANCE = new QueryResponseFormat();

  private QueryResponseFormat() {
  }

  @Override
  public String serviceName() {
    return "query";
  }

  @Override
  public QueryChunkTrailer parse(InputStream body, Consumer<QueryChunkHeader> onHeader, Consumer<byte[]> onRow) {
    return new Parser(onHeader, onRow).parse(body);
  }

  @Override
  public QueryChunkRow newRow(byte[] row) {
    return new QueryChunkRow(row);
  }

  @Override
  public QueryResponse newResponse(Request<QueryResponse> request, QueryChunkHeader header, Flux<QueryChunkRow> rows, Mono<QueryChunkTrailer> trailer) {
    return ((QueryRequest) request).decode(ResponseStatus.SUCCESS, header, rows, trailer);
  }

  @Override
  public Optional<CouchbaseException> error(QueryChunkTrailer trailer, int httpStatus, RequestContext requestContext) {
    return trailer.errors().map(it -> errorsToThrowable(it, httpStatus, requestContext));
  }

  @Override
  public Optional<RetryReason> retryReason(CouchbaseException error) {
    ErrorContext ctx = error.context();
    if (ctx instanceof QueryErrorContext) {
      QueryErrorContext errorContext = (QueryErrorContext) ctx;
      List<ErrorCodeAndMessage> errors = errorContext.errors();
      if (!errors.isEmpty()) {
        return mapError(errors.get(0));
      }
    }
    return Optional.empty();
  }

  /**
   * Check if the error code is eligible for transparent retry.
   * <p>
   * Note that while not explicitly necessary, the code calls out for prepared statement errors
   * 4040, 4050 and 4070 to NOT be retried at this level. They need to be handled at the higher
   * level since we need to clear the caches and re-prepare (which is a new op technically).
   *
   * @param error the error code to check.
   * @return has some value if eligible for retry.
   */
  private static Optional<RetryReason> mapError(ErrorCodeAndMessage error) {
    int code = error.code();

    if (code == 4040 || code == 4050 || code == 4070) {
      // prepared errors not retried on purpose here.
      return Optional.empty();
    }

    if (code == 5000 && error.message().contains("queryport.indexNotFound")) {
      return Optional.of(RetryReason.QUERY_INDEX_NOT_FOUND);
    }

    if (code == 17013) {
      return Optional.of(RetryReason.QUERY_TRANSACTION_BUFFER_FULL);
    }

    // The SDK will always try to retry if retry is set to true on a query
    // response, but due to https://issues.couchbase.com/browse/MB-50643 we
    // need to explicitly don't do that for error code 12016 to preserve
    // backwards compatibility.
    if (error.retry() && code != 12016) {
      return Optional.of(RetryReason.QUERY_ERROR_RETRYABLE);
    }

    return Optional.empty();
  }

  /**
   * Holds error codes that are special cased for prepared statements.
   */
  private static final Set<Integer> PREPARED_ERROR_CODES = setOf(4040, 4050, 4060, 4070, 4080, 4090);

  /**
   * Contains all prepared error codes that can be retried by the upper layers.
   * <p>
   * Note that this list is a strict subset of the {@link #PREPARED_ERROR_CODES}.
   */
  private static final Set<Integer> RETRYABLE_PREPARED_ERROR_CODES = setOf(4040, 4050, 4070);

  /**
   * Returns the exception for the {@code errors} field of a query response.
   */
  static CouchbaseException errorsToThrowable(
    byte[] bytes,
    int httpStatus,
    @Nullable RequestContext ctx
  ) {
    final List<ErrorCodeAndMessage> errors = bytes.length == 0
      ? Collections.emptyList()
      : ErrorCodeAndMessage.fromJsonArray(bytes);
    QueryErrorContext errorContext = new QueryErrorContext(ctx, errors, httpStatus);

    if (!errors.isEmpty()) {
      ErrorCodeAndMessage codeAndMessage = errors.get(0);
      int code = codeAndMessage.code();
      String message = codeAndMessage.message();
      int reasonCode = codeAndMessage.reason() != null
        ? (int) codeAndMessage.reason().getOrDefault("code", 0)
        : 0;

      if (code == 3000) {
        return new ParsingFailureException(errorContext);
      } else if (PREPARED_ERROR_CODES.contains(code)) {
        return new PreparedStatementFailureException(errorContext, RETRYABLE_PREPARED_ERROR_CODES.contains(code));
      } else if (code == 4300 && message.matches("^.*index .*already exist.*")) {
        return new IndexExistsException(errorContext);
      } else if (code >= 4000 && code < 5000) {
        return new PlanningFailureException(errorContext);
      } else if (code == 12004 || code == 12016 || (code == 5000 && message.matches("^.*index .+ not found.*"))
              || (code == 5000 && message.matches("^.*Index does not exist.*"))) {
        return new IndexNotFoundException(errorContext);
      } else if (code == 5000 && message.matches("^.*Index .*already exist.*")) {
        return new IndexExistsException(errorContext);
      } else if (code == 5000 && message.contains("limit for number of indexes that can be created per scope has been reached")) {
        return new QuotaLimitedException(errorContext);
      } else if (code >= 5000 && code < 6000) {
        return new InternalServerFailureException(errorContext);
      } else if (code == 12009) {
        if (message.contains("CAS mismatch") || reasonCode == 12033) {
          return new CasMismatchException(errorContext);
        } else if (reasonCode == 17014) {
          return new DocumentNotFoundException(errorContext);
        } else if (reasonCode == 17012) {
          return new DocumentExistsException(errorContext);
        } else {
          return new DmlFailureException(errorContext);
        }
      } else if ((code >= 10000 && code < 11000) || code == 13014 || code == 2120) {
        return new AuthenticationFailureException("Could not authenticate query", errorContext, null);
      } else if ((code >= 12000 && code < 13000) || (code >= 14000 && code < 15000)) {
        return new IndexFailureException(errorContext);
      } else if (code == 1065) {
        if (message.contains("query_context")) {
          return FeatureNotAvailableException.scopeLevelQuery(ServiceType.QUERY);
        }
        if (message.contains("preserve_expiry")) {
          return FeatureNotAvailableException.queryPreserveExpiry();
        }
        if (message.contains("use_replica")) {
          return FeatureNotAvailableException.queryUseReplica(message);
        }
      } else if (code == 1080) {
        // This can happen when the server starts streaming responses - at this point our timeout is already
        // canceled. But then the streaming takes longer than the configured timeout, in which case the query
        // engine will proactively send us a timeout and we need to convert it.
        return new UnambiguousTimeoutException(
          "Query timed out while streaming/receiving rows",
          new CancellationErrorContext(errorContext)
        );
      } else if (code == 1191 || code == 1192 || code == 1193 || code == 1194) {
        return new RateLimitedException(errorContext);
      } else if (code == 1197) {
        return new FeatureNotAvailableException("This server requires that scope.query() be used rather than cluster.query()");
      } else if (code == 3230) {
        String feature = null;
        if (message.contains("Advisor") || message.contains("Advise")) {
          feature = "Query Index Advisor";
        } else if (message.contains("Window")) {
          feature = "Query Window Functions";
        }
        return FeatureNotAvailableException.communityEdition(feature);
      }
    }
    if (httpStatus == 400) {
      return new InvalidArgumentException("Query service reported an invalid argument", null, errorContext);
    }
    return new CouchbaseException("Unknown query error", errorContext);
  }


  /**
   * Parses one response body. Holds the state of that parse, so each response gets a new instance.
   */
  private static final class Parser {
    private final Consumer<QueryChunkHeader> headerCallback;
    private final Consumer<byte[]> rowCallback;

    private boolean headerCompleted = false;

    private @Nullable String requestId;
    private byte @Nullable [] signature;
    private @Nullable String clientContextId;
    private @Nullable String prepared;

    private @Nullable String status;
    private byte @Nullable [] metrics;
    private byte @Nullable [] warnings;
    private byte @Nullable [] errors;
    private byte @Nullable [] profile;

    private Parser(Consumer<QueryChunkHeader> headerCallback, Consumer<byte[]> rowCallback) {
      this.headerCallback = requireNonNull(headerCallback);
      this.rowCallback = requireNonNull(rowCallback);
    }

    private QueryChunkTrailer parse(InputStream is) {
      try (JsonStreamParser parser = newStreamParser()) {
        parser.feed(is);
        parser.endOfInput();
      }
      return trailer();
    }

    private JsonStreamParser newStreamParser() {
      return JsonStreamParser.builder()
        .doOnValue("/requestID", v -> requestId = v.readString())
        .doOnValue("/signature", v -> signature = v.bytes())
        .doOnValue("/clientContextID", v -> clientContextId = v.readString())
        .doOnValue("/prepared", v -> prepared = v.readString())
        .doOnValue("/results/-", v -> {
          maybeCompleteHeader();
          rowCallback.accept(v.bytes());
        })
        .doOnValue("/status", v -> {
          maybeCompleteHeader();
          status = v.readString();
        })
        .doOnValue("/metrics", v -> metrics = v.bytes())
        .doOnValue("/profile", v -> profile = v.bytes())
        .doOnValue("/errors", v -> errors = v.bytes())
        .doOnValue("/warnings", v -> warnings = v.bytes())
        .build();
    }

    private void maybeCompleteHeader() {
      if (headerCompleted) return;

      headerCompleted = true;

      QueryChunkHeader header = new QueryChunkHeader(
        requestId,
        Optional.ofNullable(clientContextId),
        Optional.ofNullable(signature),
        Optional.ofNullable(prepared)
      );

      headerCallback.accept(header);
    }

    private QueryChunkTrailer trailer() {
      return new QueryChunkTrailer(
        status,
        Optional.ofNullable(metrics),
        Optional.ofNullable(warnings),
        Optional.ofNullable(errors),
        Optional.ofNullable(profile)
      );
    }
  }
}

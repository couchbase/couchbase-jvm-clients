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

package com.couchbase.client.core.io.netty.search;

import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.InternalServerFailureException;
import com.couchbase.client.core.error.QuotaLimitedException;
import com.couchbase.client.core.error.RateLimitedException;
import com.couchbase.client.core.error.context.SearchErrorContext;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryReason;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the search error translation accessors that the OkHttp-based search service uses.
 */
class SearchErrorTranslationTest {

  private final RequestContext ctx = mock(RequestContext.class);

  private CouchbaseException queryError(int httpStatus, String error) {
    return SearchChunkResponseParser.errorsToThrowable(error.getBytes(UTF_8), HttpResponseStatus.valueOf(httpStatus), ctx);
  }

  private CouchbaseException managementError(int httpStatus, String content) {
    Request<?> request = mock(Request.class);
    when(request.context()).thenReturn(ctx);
    return (CouchbaseException) NonChunkedSearchMessageHandler.errorToThrowable(HttpResponseStatus.valueOf(httpStatus), content, request);
  }

  private static <T extends CouchbaseException> T assertType(Class<T> expected, CouchbaseException actual) {
    assertEquals(expected, actual.getClass(), "unexpected exception: " + actual);
    return expected.cast(actual);
  }

  // ---- Search query errors ----

  @Test
  void queryIndexNotFound() {
    assertType(IndexNotFoundException.class, queryError(400, "rest_auth: preparePerms, err: index not found"));
  }

  @Test
  void queryInternalServerError() {
    assertType(InternalServerFailureException.class, queryError(500, "something broke"));
  }

  @Test
  void queryAuthenticationFailure() {
    assertType(AuthenticationFailureException.class, queryError(401, "unauthorized"));
    assertType(AuthenticationFailureException.class, queryError(403, "forbidden"));
  }

  @Test
  void queryIndexQuota() {
    assertType(QuotaLimitedException.class, queryError(400, "num_fts_indexes (active + pending) over limit"));
  }

  @Test
  void queryRateLimited() {
    for (String limit : new String[]{"num_concurrent_requests", "num_queries_per_min", "ingress_mib_per_min", "egress_mib_per_min"}) {
      assertType(RateLimitedException.class, queryError(429, "limit exceeded: " + limit));
    }
  }

  @Test
  void query429WithoutRateLimitIsGeneric() {
    CouchbaseException e = assertType(CouchbaseException.class, queryError(429, "too busy"));
    assertTrue(e.getMessage().contains("Unknown search error: too busy"), e.getMessage());
  }

  @Test
  void queryUnknownError() {
    CouchbaseException e = assertType(CouchbaseException.class, queryError(400, "bad query"));
    assertTrue(e.getMessage().contains("Unknown search error: bad query"), e.getMessage());
  }

  @Test
  void queryErrorContext() {
    CouchbaseException e = queryError(400, "bad query");
    SearchErrorContext context = (SearchErrorContext) e.context();
    assertEquals(400, context.httpStatus());
    assertEquals("bad query", context.content());
    assertEquals(ResponseStatus.INVALID_ARGS, context.responseStatus());
    assertSame(ctx, context.requestContext());
  }

  @Test
  void queryEmptyOrMissingError() {
    assertType(InternalServerFailureException.class, SearchChunkResponseParser.errorsToThrowable(null, HttpResponseStatus.valueOf(500), ctx));
    CouchbaseException e = assertType(CouchbaseException.class, SearchChunkResponseParser.errorsToThrowable(new byte[0], HttpResponseStatus.valueOf(400), ctx));
    assertEquals("", ((SearchErrorContext) e.context()).content());
  }

  // ---- Management (non-streaming) errors ----

  @Test
  void managementIndexMissingForUpdate() {
    IndexNotFoundException e = assertType(IndexNotFoundException.class, managementError(400, "index missing for update"));
    assertTrue(e.getMessage().contains("during an update on upsert"), e.getMessage());
  }

  @Test
  void managementIndexNotFound() {
    IndexNotFoundException e = assertType(IndexNotFoundException.class, managementError(400, "index not found"));
    assertTrue(e.getMessage().contains("Index not found"), e.getMessage());
  }

  @Test
  void managementIndexExists() {
    assertType(IndexExistsException.class, managementError(400, "index with the same name already exists"));
  }

  @Test
  void managementIndexQuota() {
    assertType(QuotaLimitedException.class, managementError(400, "num_fts_indexes over limit"));
  }

  @Test
  void managementRateLimited() {
    assertType(RateLimitedException.class, managementError(429, "num_concurrent_requests"));
  }

  @Test
  void managementUnknownError() {
    // Including 500 and auth failures, which the management mapping doesn't single out.
    for (int status : new int[]{400, 401, 404, 429, 500}) {
      CouchbaseException e = assertType(CouchbaseException.class, managementError(status, "Page not found"));
      assertTrue(e.getMessage().contains("Unknown search error: Page not found"), e.getMessage());
      assertEquals(status, ((SearchErrorContext) e.context()).httpStatus());
    }
  }

  // ---- Retry ----

  @Test
  void retriesTooManyRequestsUnlessRateOrQuotaLimited() {
    assertEquals(Optional.of(RetryReason.SEARCH_TOO_MANY_REQUESTS), ChunkedSearchMessageHandler.retryReason(queryError(429, "too busy")));

    assertEquals(Optional.empty(), ChunkedSearchMessageHandler.retryReason(queryError(429, "num_concurrent_requests")));
    assertEquals(Optional.empty(), ChunkedSearchMessageHandler.retryReason(queryError(400, "num_fts_indexes")));
  }

  @Test
  void doesNotRetryOtherErrors() {
    assertEquals(Optional.empty(), ChunkedSearchMessageHandler.retryReason(queryError(500, "something broke")));
    assertEquals(Optional.empty(), ChunkedSearchMessageHandler.retryReason(queryError(400, "bad query")));
    assertEquals(Optional.empty(), ChunkedSearchMessageHandler.retryReason(new CouchbaseException("no context")));
  }
}

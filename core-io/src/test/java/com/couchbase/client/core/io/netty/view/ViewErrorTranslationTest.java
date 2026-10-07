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

package com.couchbase.client.core.io.netty.view;

import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.view.ViewError;
import com.couchbase.client.core.error.ViewNotFoundException;
import com.couchbase.client.core.error.context.ViewErrorContext;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryReason;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests the view error translation accessors that the OkHttp-based view service uses.
 */
class ViewErrorTranslationTest {

  private final RequestContext ctx = mock(RequestContext.class);

  private CouchbaseException error(int httpStatus, String error, String reason) {
    return ViewChunkResponseParser.errorToThrowable(new ViewError(error, reason), HttpResponseStatus.valueOf(httpStatus), ctx);
  }

  private Optional<RetryReason> retry(int httpStatus, String error, String reason) {
    return ChunkedViewMessageHandler.retryReason(error(httpStatus, error, reason));
  }

  @Test
  void viewNotFound() {
    assertEquals(ViewNotFoundException.class, error(404, "whatever", "whatever").getClass());
    assertEquals(ViewNotFoundException.class, error(500, "not_found", "whatever").getClass());
    assertEquals(ViewNotFoundException.class, error(500, "error", "{not_found, missing_named_view}").getClass());
  }

  @Test
  void unknownError() {
    CouchbaseException e = error(400, "query_parse_error", "bad stale value");
    assertEquals(CouchbaseException.class, e.getClass());
    assertTrue(e.getMessage().startsWith("Unknown view error: ViewError{error='query_parse_error', reason='bad stale value'}"), e.getMessage());

    ViewErrorContext context = (ViewErrorContext) e.context();
    assertEquals(400, context.httpStatus());
    assertEquals(ResponseStatus.INVALID_ARGS, context.responseStatus());
    assertSame(ctx, context.requestContext());
  }

  @Test
  void retries() {
    assertEquals(Optional.of(RetryReason.VIEWS_TEMPORARY_FAILURE), retry(404, "not_found", "missing"));
    assertEquals(Optional.empty(), retry(404, "not_found", "deleted"));

    assertEquals(Optional.of(RetryReason.VIEWS_TEMPORARY_FAILURE), retry(500, "error", "something"));
    assertEquals(Optional.empty(), retry(500, "error", "{not_found, missing_named_view}"));
    assertEquals(Optional.empty(), retry(500, "error", "badarg"));

    assertEquals(Optional.of(RetryReason.VIEWS_NO_ACTIVE_PARTITION), retry(302, "e", "r"));

    for (int status : new int[]{300, 301, 303, 307, 401, 408, 409, 412, 416, 417, 501, 502, 503, 504}) {
      assertEquals(Optional.of(RetryReason.VIEWS_TEMPORARY_FAILURE), retry(status, "e", "r"), "status " + status);
    }
    for (int status : new int[]{200, 400, 403, 429}) {
      assertEquals(Optional.empty(), retry(status, "e", "r"), "status " + status);
    }
    assertEquals(Optional.empty(), ChunkedViewMessageHandler.retryReason(new CouchbaseException("no context")));
  }
}

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

package com.couchbase.client.core.io.netty.analytics;

import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.error.CompilationFailureException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.InternalServerFailureException;
import com.couchbase.client.core.error.JobQueueFullException;
import com.couchbase.client.core.error.LinkExistsException;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.core.error.context.AnalyticsErrorContext;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.retry.RetryReason;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests the analytics error translation accessors that the OkHttp-based analytics service uses.
 */
class AnalyticsErrorTranslationTest {

  private final RequestContext ctx = mock(RequestContext.class);

  private CouchbaseException error(int httpStatus, String errors) {
    return AnalyticsChunkResponseParser.errorsToThrowable(errors.getBytes(UTF_8), ctx, HttpResponseStatus.valueOf(httpStatus));
  }

  private static String errorWithCode(int code) {
    return "[{\"code\":" + code + ",\"msg\":\"oops\"}]";
  }

  @Test
  void mapsErrorCodes() {
    assertEquals(InternalServerFailureException.class, error(500, errorWithCode(25000)).getClass());
    assertEquals(TemporaryFailureException.class, error(503, errorWithCode(23000)).getClass());
    assertEquals(TemporaryFailureException.class, error(503, errorWithCode(23003)).getClass());
    assertEquals(JobQueueFullException.class, error(503, errorWithCode(23007)).getClass());
    assertEquals(CompilationFailureException.class, error(400, errorWithCode(24999)).getClass());

    CouchbaseException unknown = error(400, errorWithCode(99999));
    assertEquals(CouchbaseException.class, unknown.getClass());
    assertTrue(unknown.getMessage().startsWith("Unknown analytics error: "), unknown.getMessage());
    assertEquals(99999, ((AnalyticsErrorContext) unknown.context()).errors().get(0).code());
  }

  @Test
  void parsesPlaintextBodiesFromOlderServers() {
    // For non-streaming requests, the whole body is passed in.
    assertEquals(LinkExistsException.class, error(400, "CBAS0026: Link already exists").getClass());
  }

  @Test
  void noErrors() {
    assertTrue(error(500, "").getMessage().startsWith("Unknown analytics error"));
  }

  @Test
  void retries() {
    assertEquals(Optional.of(RetryReason.ANALYTICS_TEMPORARY_FAILURE), AnalyticsMessageHandler.retryReason(error(503, errorWithCode(23000))));
    assertEquals(Optional.of(RetryReason.ANALYTICS_TEMPORARY_FAILURE), AnalyticsMessageHandler.retryReason(error(503, errorWithCode(23007))));
    assertEquals(Optional.empty(), AnalyticsMessageHandler.retryReason(error(400, errorWithCode(24000))));
    assertEquals(Optional.empty(), AnalyticsMessageHandler.retryReason(new CouchbaseException("no context")));
  }
}

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

import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.error.QuotaLimitedException;
import com.couchbase.client.core.error.RateLimitedException;
import com.couchbase.client.core.error.context.ManagerErrorContext;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the manager error translation accessor that the OkHttp-based manager service uses.
 */
class ManagerErrorTranslationTest {

  private Exception error(int httpStatus, String content) {
    Request<?> request = mock(Request.class);
    when(request.context()).thenReturn(mock(RequestContext.class));
    return OkHttpManagerService.errorToThrowable(HttpResponseStatus.valueOf(httpStatus), content, request);
  }

  @Test
  void communityEdition() {
    assertTrue(error(400, "Magma is supported in enterprise edition only").getMessage().contains("Storage Backend: Magma"));
    assertTrue(error(400, "Compression mode is supported in enterprise edition only").getMessage().contains("Compression Mode"));
    assertEquals(FeatureNotAvailableException.class, error(400, "This http API endpoint requires enterprise edition").getClass());
  }

  @Test
  void rateAndQuotaLimits() {
    assertEquals(RateLimitedException.class, error(429, "num_concurrent_requests").getClass());
    assertEquals(RateLimitedException.class, error(429, "ingress").getClass());
    assertEquals(RateLimitedException.class, error(429, "egress").getClass());
    assertEquals(QuotaLimitedException.class, error(429, "Maximum number of collections has been reached for scope").getClass());
  }

  @Test
  void badRequestIsAnInvalidArgument() {
    // JVMCBC-1800: keeps the HttpStatusCodeException as the cause, so callers can still inspect the response.
    Exception e = error(400, "something else");
    assertEquals(InvalidArgumentException.class, e.getClass());
    HttpStatusCodeException cause = (HttpStatusCodeException) e.getCause();
    assertEquals(400, ((ManagerErrorContext) cause.context()).httpStatus());
  }

  @Test
  void otherErrors() {
    for (int status : new int[]{404, 429, 500}) {
      Exception e = error(status, "something else");
      assertEquals(HttpStatusCodeException.class, e.getClass());
      ManagerErrorContext context = (ManagerErrorContext) ((HttpStatusCodeException) e).context();
      assertEquals(status, context.httpStatus());
      assertEquals("something else", context.content());
    }
  }
}

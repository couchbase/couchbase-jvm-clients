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

package com.couchbase.client.core.io.netty.eventing;

import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.EventingFunctionCompilationFailureException;
import com.couchbase.client.core.error.EventingFunctionDeployedException;
import com.couchbase.client.core.error.EventingFunctionIdenticalKeyspaceException;
import com.couchbase.client.core.error.EventingFunctionNotBootstrappedException;
import com.couchbase.client.core.error.EventingFunctionNotDeployedException;
import com.couchbase.client.core.error.EventingFunctionNotFoundException;
import com.couchbase.client.core.error.ScopeNotFoundException;
import com.couchbase.client.core.error.context.EventingErrorContext;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.ResponseStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the eventing error translation accessor that the OkHttp-based eventing service uses.
 */
class EventingErrorTranslationTest {

  private CouchbaseException error(int httpStatus, String content) {
    Request<?> request = mock(Request.class);
    when(request.context()).thenReturn(mock(RequestContext.class));
    return (CouchbaseException) NonChunkedEventingMessageHandler.errorToThrowable(HttpResponseStatus.valueOf(httpStatus), content, request);
  }

  @Test
  void mapsErrorNames() {
    assertEquals(EventingFunctionNotFoundException.class, error(404, "ERR_APP_NOT_FOUND_TS").getClass());
    assertEquals(EventingFunctionNotDeployedException.class, error(400, "ERR_APP_NOT_DEPLOYED").getClass());
    assertEquals(EventingFunctionCompilationFailureException.class, error(400, "ERR_HANDLER_COMPILATION").getClass());
    assertEquals(CollectionNotFoundException.class, error(400, "ERR_COLLECTION_MISSING").getClass());
    assertEquals(EventingFunctionIdenticalKeyspaceException.class, error(400, "ERR_SRC_MB_SAME").getClass());
    assertEquals(EventingFunctionNotBootstrappedException.class, error(400, "ERR_APP_NOT_BOOTSTRAPPED").getClass());
    assertEquals(EventingFunctionDeployedException.class, error(400, "ERR_APP_NOT_UNDEPLOYED").getClass());
    assertEquals(BucketNotFoundException.class, error(400, "ERR_BUCKET_MISSING").getClass());
    assertEquals(ScopeNotFoundException.class, error(400, "ERR_BUCKET_MISSING: Scope Not Defined").getClass());
  }

  @Test
  void unknownError() {
    CouchbaseException e = error(500, "something else");
    assertEquals(CouchbaseException.class, e.getClass());
    assertTrue(e.getMessage().startsWith("Unknown eventing error: something else"), e.getMessage());

    EventingErrorContext context = (EventingErrorContext) e.context();
    assertEquals(500, context.httpStatus());
    assertEquals(ResponseStatus.INTERNAL_SERVER_ERROR, context.responseStatus());
  }

  @Test
  void jsonPropertiesGoInErrorContext() {
    EventingErrorContext context = (EventingErrorContext) error(404, "{\"name\":\"ERR_APP_NOT_FOUND_TS\",\"code\":24}").context();
    assertEquals("ERR_APP_NOT_FOUND_TS", context.responseProperties().get("name"));
    assertEquals(24, context.responseProperties().get("code"));

    // Best effort: no properties if the body isn't a JSON object.
    assertTrue(((EventingErrorContext) error(500, "not json").context()).responseProperties().isEmpty());
  }
}

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

import okhttp3.RequestBody;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The request bodies of {@link com.couchbase.client.core.endpoint.http.CoreHttpRequest}s
 * (see {@link AbstractOkHttpService#requestBody}).
 */
class CoreHttpRequestBodyTest {
  private static final byte[] CONTENT = "{\"name\":\"example\"}".getBytes(UTF_8);
  private static final byte[] EMPTY = new byte[0];

  @Test
  void requestWithContentHasIt() throws Exception {
    RequestBody body = AbstractOkHttpService.requestBody("PUT", CONTENT);
    assertNotNull(body);
    assertEquals(CONTENT.length, body.contentLength());
  }

  @Test
  void emptyPostPutAndPatchGetAnEmptyBody() throws Exception {
    // OkHttp requires a body for these, even if it's empty (for example, deploying an eventing function).
    for (String method : new String[]{"POST", "PUT", "PATCH"}) {
      RequestBody body = AbstractOkHttpService.requestBody(method, EMPTY);
      assertNotNull(body, method);
      assertEquals(0, body.contentLength(), method);
      new okhttp3.Request.Builder().url("http://example/").method(method, body).build(); // OkHttp accepts it
    }
  }

  @Test
  void emptyGetHeadAndDeleteHaveNoBody() {
    for (String method : new String[]{"GET", "HEAD", "DELETE"}) {
      assertNull(AbstractOkHttpService.requestBody(method, EMPTY), method);
      new okhttp3.Request.Builder().url("http://example/").method(method, null).build(); // OkHttp accepts it
    }
  }
}

/*
 * Copyright (c) 2018 Couchbase, Inc.
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


package com.couchbase.client.core.msg;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Verifies the functionality for the {@link RequestContext}.
 *
 * @since 2.0.0
 */
class RequestContextTest {

  @Test
  void requestCancellation() {
    Request<?> request = mock(Request.class);
    Core core = mock(Core.class);
    RequestContext ctx = new RequestContext(new CoreContext(core, 1, null), request);

    ctx.cancel();
    verify(request, times(1)).cancel(CancellationReason.CANCELLED_VIA_CONTEXT);
  }

  @Test
  void customPayloadCanBeAttached() {
    Request<?> request = mock(Request.class);
    Core core = mock(Core.class);
    RequestContext ctx = new RequestContext(new CoreContext(core, 1, null), request);
    assertNull(ctx.payload());

    Map<String, Object> payload = new HashMap<>();
    payload.put("foo", true);
    ctx.payload(payload);

    assertEquals(payload, ctx.payload());
  }

}
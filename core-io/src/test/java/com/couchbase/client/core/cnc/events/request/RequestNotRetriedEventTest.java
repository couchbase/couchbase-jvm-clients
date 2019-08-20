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

package com.couchbase.client.core.cnc.events.request;

import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.retry.RetryReason;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class RequestNotRetriedEventTest {

  @Test
  void verifyDescription() {
    RequestNotRetriedEvent event = new RequestNotRetriedEvent(GetRequest.class, mock(RequestContext.class), RetryReason.UNKNOWN);
    assertEquals("Request GetRequest not retried per RetryStrategy (Reason: UNKNOWN)", event.description());
  }

}
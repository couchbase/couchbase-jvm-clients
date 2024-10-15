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

package com.couchbase.client.core;

import com.couchbase.client.core.deps.io.netty.util.Timeout;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.couchbase.client.core.util.MockUtil.mockCore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TimerTest {

  @Test
  @SuppressWarnings("unchecked")
  void performsBackpressureWhenOverLimit() throws Exception {
    Timer timer = Timer.createAndStart(2);
    try {
      Core core = mockCore();
      assertEquals(0, timer.outstandingForRetry());

      Request<? extends Response> request = mock(Request.class);
      timer.scheduleForRetry(core, request, Duration.ofMillis(500));
      verify(request, never()).cancel(CancellationReason.TOO_MANY_REQUESTS_IN_RETRY);
      assertEquals(1, timer.outstandingForRetry());

      request = mock(Request.class);
      timer.scheduleForRetry(core, request, Duration.ofMillis(500));
      verify(request, never()).cancel(CancellationReason.TOO_MANY_REQUESTS_IN_RETRY);
      assertEquals(2, timer.outstandingForRetry());

      request = mock(Request.class);
      timer.scheduleForRetry(core, request, Duration.ofMillis(500));
      verify(request, times(1)).cancel(CancellationReason.TOO_MANY_REQUESTS_IN_RETRY);
      assertEquals(2, timer.outstandingForRetry());

      Thread.sleep(1000);

      assertEquals(0, timer.outstandingForRetry());
      request = mock(Request.class);
      timer.scheduleForRetry(core, request, Duration.ofMillis(500));
      verify(request, never()).cancel(CancellationReason.TOO_MANY_REQUESTS_IN_RETRY);
      assertEquals(1, timer.outstandingForRetry());
    } finally {
      timer.stop();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void assignsRegistrationToRequest() {
    Timer timer = Timer.createAndStart(2);
    try {
      Request<Response> request = mock(Request.class);
      timer.register(request);
      verify(request, times(1)).timeoutRegistration(any(Timeout.class));
    } finally {
      timer.stop();
    }
  }

}

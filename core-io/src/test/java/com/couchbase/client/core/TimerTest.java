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

import com.couchbase.client.core.msg.BaseHttpRequest;
import com.couchbase.client.core.msg.BlockingCaller;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.core.util.MockUtil.mockCore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
      verify(request, times(1)).timeoutRegistration(any(Timer.Registration.class));
    } finally {
      timer.stop();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void stopCancelsRequestWaitingForTimeout() {
    Timer timer = Timer.createAndStart(2);
    Request<Response> request = mock(Request.class);
    when(request.timeout()).thenReturn(Duration.ofMinutes(1));
    timer.register(request);

    timer.stop();
    verify(request, times(1)).cancel(CancellationReason.SHUTDOWN);
  }

  @Test
  @SuppressWarnings("unchecked")
  void stopCancelsRequestWaitingForRetry() {
    Timer timer = Timer.createAndStart(2);
    Core core = mockCore();
    Request<? extends Response> request = mock(Request.class);
    timer.scheduleForRetry(core, request, Duration.ofMinutes(1));

    timer.stop();
    verify(request, times(1)).cancel(CancellationReason.SHUTDOWN);
    verify(core, never()).send(any(), anyBoolean());
  }

  @Test
  @SuppressWarnings("unchecked")
  void cancelsRequestsScheduledAfterStop() {
    Timer timer = Timer.createAndStart(2);
    timer.stop();

    Request<Response> request = mock(Request.class);
    timer.register(request);
    verify(request, times(1)).cancel(CancellationReason.SHUTDOWN);

    Core core = mockCore();
    Request<? extends Response> retryRequest = mock(Request.class);
    timer.scheduleForRetry(core, retryRequest, Duration.ofMillis(1));
    verify(retryRequest, times(1)).cancel(CancellationReason.SHUTDOWN);
    assertEquals(0, timer.outstandingForRetry());
  }

  @Test
  void handsRetriesToTheRequestsBlockingCaller() {
    Timer timer = Timer.createAndStart(2);
    try {
      Core core = mockCore();
      BlockingCaller caller = new BlockingCaller();
      BaseHttpRequest<?> request = mock(BaseHttpRequest.class);
      when(request.blockingCaller()).thenReturn(caller);

      CompletableFuture<Void> result = new CompletableFuture<>();
      AtomicReference<Thread> retriedOn = new AtomicReference<>();
      doAnswer(invocation -> {
        retriedOn.set(Thread.currentThread());
        result.complete(null);
        return null;
      }).when(core).send(request, false);

      timer.scheduleForRetry(core, request, Duration.ofMillis(10));
      assertEquals(1, timer.outstandingForRetry()); // still counts towards the limit

      caller.attach(request);
      caller.await(result);
      assertSame(Thread.currentThread(), retriedOn.get());
      assertEquals(0, timer.outstandingForRetry());
    } finally {
      timer.stop();
    }
  }

  @Test
  void dropsARetryWhenTheBlockingCallerStopsWaiting() {
    Timer timer = Timer.createAndStart(2);
    try {
      Core core = mockCore();
      BlockingCaller caller = new BlockingCaller();
      BaseHttpRequest<?> request = mock(BaseHttpRequest.class); // never completes
      when(request.blockingCaller()).thenReturn(caller);

      timer.scheduleForRetry(core, request, Duration.ofMinutes(1));
      assertEquals(1, timer.outstandingForRetry());

      caller.attach(request);
      caller.await(CompletableFuture.completedFuture(null)); // stops waiting, dropping the retry
      assertEquals(0, timer.outstandingForRetry(), "should no longer count towards the limit");
      verify(request).cancel(CancellationReason.STOPPED_LISTENING); // since it wasn't done
      verify(core, never()).send(any(), anyBoolean());

      // Now that the caller isn't waiting, the timer retries the request itself.
      AtomicReference<Thread> retriedOn = new AtomicReference<>();
      doAnswer(invocation -> {
        retriedOn.set(Thread.currentThread());
        return null;
      }).when(core).send(request, false);
      timer.scheduleForRetry(core, request, Duration.ofMillis(1));
      verify(core, timeout(5000)).send(request, false);
      assertNotSame(Thread.currentThread(), retriedOn.get());
    } finally {
      timer.stop();
    }
  }

}

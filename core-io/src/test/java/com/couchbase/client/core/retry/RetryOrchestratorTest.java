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

package com.couchbase.client.core.retry;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.Timer;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.util.Utils.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link RetryOrchestrator}.
 */
class RetryOrchestratorTest {

  @Test
  void ignoreAlreadyCompletedRequest() {
    Request<?> request = mock(Request.class);
    when(request.completed()).thenReturn(true);

    RetryOrchestrator.maybeRetry(null, request);

    verify(request, times(1)).completed();
    verifyNoMoreInteractions(request);
  }

  @Test
  @SuppressWarnings({"unchecked"})
  void cancelIfNoMoreRetriesAllowed() {
    RetryStrategy retryStrategy = mock(RetryStrategy.class);
    when(retryStrategy.shouldRetry(any(Request.class))).thenReturn(Optional.empty());
    Request<?> request = mock(Request.class);
    when(request.completed()).thenReturn(false);
    when(request.retryStrategy()).thenReturn(retryStrategy);

    RetryOrchestrator.maybeRetry(null, request);

    verify(request, never()).context();
    verify(request, times(1)).cancel(CancellationReason.NO_MORE_RETRIES);
  }

  @Test
  @SuppressWarnings({"unchecked"})
  void retryWithDelay() {
    Timer timer = Timer.createAndStart();

    RetryStrategy retryStrategy = mock(RetryStrategy.class);
    when(retryStrategy.shouldRetry(any(Request.class))).thenReturn(
      Optional.of(Duration.ofMillis(200))
    );
    Request<?> request = mock(Request.class);
    RequestContext requestContext = mock(RequestContext.class);
    when(request.completed()).thenReturn(false);
    when(request.context()).thenReturn(requestContext);
    when(request.retryStrategy()).thenReturn(retryStrategy);

    Core core = mock(Core.class);
    CoreEnvironment env = mock(CoreEnvironment.class);
    when(env.timer()).thenReturn(timer);
    CoreContext ctx = new CoreContext(core, 1, env);

    long start = System.nanoTime();
    RetryOrchestrator.maybeRetry(ctx, request);

    verify(requestContext, times(1)).incrementRetryAttempt();
    verify(request, never()).cancel(CancellationReason.NO_MORE_RETRIES);

    waitUntilCondition(() -> !Mockito.mockingDetails(core).getInvocations().isEmpty());

    long end = System.nanoTime();
    verify(core, times(1)).send(request, false);
    verify(core, never()).send(request);
    assertTrue(TimeUnit.NANOSECONDS.toMillis(end - start) >= 200);
    timer.stop();
  }

}
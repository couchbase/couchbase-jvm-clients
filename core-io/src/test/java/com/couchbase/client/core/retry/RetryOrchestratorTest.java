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
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.request.RequestNotRetriedEvent;
import com.couchbase.client.core.cnc.events.request.RequestRetryScheduledEvent;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.util.SimpleEventBus;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
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

    RetryOrchestrator.maybeRetry(null, request, RetryReason.UNKNOWN);

    verify(request, times(1)).completed();
    verifyNoMoreInteractions(request);
  }

  @Test
  @SuppressWarnings({"unchecked"})
  void cancelIfNoMoreRetriesAllowed() {
    RetryStrategy retryStrategy = mock(RetryStrategy.class);
    when(retryStrategy.shouldRetry(any(Request.class), any(RetryReason.class)))
      .thenReturn(CompletableFuture.completedFuture(RetryAction.noRetry()));
    Request<?> request = mock(Request.class);
    when(request.completed()).thenReturn(false);
    when(request.retryStrategy()).thenReturn(retryStrategy);
    RequestContext requestContext = mock(RequestContext.class);
    when(request.context()).thenReturn(requestContext);

    CoreEnvironment env = mock(CoreEnvironment.class);
    SimpleEventBus eventBus = new SimpleEventBus(true);
    when(env.eventBus()).thenReturn(eventBus);
    CoreContext context = new CoreContext(mock(Core.class), 1, env, mock(Authenticator.class));
    RetryOrchestrator.maybeRetry(context, request, RetryReason.UNKNOWN);

    verify(request, times(1)).cancel(CancellationReason.noMoreRetries(RetryReason.UNKNOWN));

    assertEquals(1, eventBus.publishedEvents().size());
    RequestNotRetriedEvent retryEvent = (RequestNotRetriedEvent) eventBus.publishedEvents().get(0);
    assertEquals(Event.Severity.INFO, retryEvent.severity());
    assertEquals(Event.Category.REQUEST.path(), retryEvent.category());
    assertEquals(requestContext, retryEvent.context());
  }

  @Test
  @SuppressWarnings({"unchecked"})
  void retryWithDelay() {
    Timer timer = Timer.createAndStart(CoreEnvironment.DEFAULT_MAX_NUM_REQUESTS_IN_RETRY);

    RetryStrategy retryStrategy = mock(RetryStrategy.class);
    when(retryStrategy.shouldRetry(any(Request.class), any(RetryReason.class))).thenReturn(
      CompletableFuture.completedFuture(RetryAction.withDuration(Duration.ofMillis(200)))
    );
    Request<?> request = mock(Request.class);
    RequestContext requestContext = mock(RequestContext.class);
    when(request.completed()).thenReturn(false);
    when(request.context()).thenReturn(requestContext);
    when(request.retryStrategy()).thenReturn(retryStrategy);
    when(request.absoluteTimeout()).thenReturn(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(2500));

    Core core = mock(Core.class);
    CoreEnvironment env = mock(CoreEnvironment.class);
    SimpleEventBus eventBus = new SimpleEventBus(true);
    when(env.timer()).thenReturn(timer);
    when(env.eventBus()).thenReturn(eventBus);

    CoreContext ctx = new CoreContext(core, 1, env, mock(Authenticator.class));

    long start = System.nanoTime();
    RetryOrchestrator.maybeRetry(ctx, request, RetryReason.UNKNOWN);

    verify(requestContext, times(1))
      .incrementRetryAttempts(Duration.ofMillis(200), RetryReason.UNKNOWN);
    verify(request, never()).cancel(CancellationReason.noMoreRetries(RetryReason.UNKNOWN));

    waitUntilCondition(() -> !Mockito.mockingDetails(core).getInvocations().isEmpty());

    long end = System.nanoTime();
    verify(core, times(1)).send(request, false);
    verify(core, never()).send(request);
    assertTrue(TimeUnit.NANOSECONDS.toMillis(end - start) >= 200);
    timer.stop();

    assertEquals(1, eventBus.publishedEvents().size());
    RequestRetryScheduledEvent retryEvent = (RequestRetryScheduledEvent) eventBus.publishedEvents().get(0);
    assertEquals(Event.Severity.DEBUG, retryEvent.severity());
    assertEquals(Event.Category.REQUEST.path(), retryEvent.category());
    assertEquals(requestContext, retryEvent.context());
    assertEquals(RetryReason.UNKNOWN, retryEvent.retryReason());
  }

  @Test
  @SuppressWarnings({"unchecked"})
  void capsRetryDelay() {
    Timer timer = Timer.createAndStart(CoreEnvironment.DEFAULT_MAX_NUM_REQUESTS_IN_RETRY);

    RetryStrategy retryStrategy = mock(RetryStrategy.class);
    when(retryStrategy.shouldRetry(any(Request.class), any(RetryReason.class))).thenReturn(
      CompletableFuture.completedFuture(RetryAction.withDuration(Duration.ofMillis(200)))
    );
    Request<?> request = mock(Request.class);
    RequestContext requestContext = mock(RequestContext.class);
    when(request.completed()).thenReturn(false);
    when(request.context()).thenReturn(requestContext);
    when(request.retryStrategy()).thenReturn(retryStrategy);
    when(request.absoluteTimeout()).thenAnswer(invocationOnMock -> System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(10));

    Core core = mock(Core.class);
    CoreEnvironment env = mock(CoreEnvironment.class);
    SimpleEventBus eventBus = new SimpleEventBus(true);
    when(env.timer()).thenReturn(timer);
    when(env.eventBus()).thenReturn(eventBus);

    CoreContext ctx = new CoreContext(core, 1, env, mock(Authenticator.class));

    long start = System.nanoTime();
    RetryOrchestrator.maybeRetry(ctx, request, RetryReason.UNKNOWN);

    verify(requestContext, never())
      .incrementRetryAttempts((Duration.ofMillis(200)), RetryReason.UNKNOWN);
    verify(request, never()).cancel(CancellationReason.noMoreRetries(RetryReason.UNKNOWN));

    waitUntilCondition(() -> !Mockito.mockingDetails(core).getInvocations().isEmpty());

    long end = System.nanoTime();
    verify(core, times(1)).send(request, false);
    verify(core, never()).send(request);
    assertTrue(TimeUnit.NANOSECONDS.toMillis(end - start) < 200);
    timer.stop();

    assertEquals(1, eventBus.publishedEvents().size());
    RequestRetryScheduledEvent retryEvent = (RequestRetryScheduledEvent) eventBus.publishedEvents().get(0);
    assertEquals(Event.Severity.DEBUG, retryEvent.severity());
    assertEquals(Event.Category.REQUEST.path(), retryEvent.category());
    assertEquals(requestContext, retryEvent.context());
    assertEquals(RetryReason.UNKNOWN, retryEvent.retryReason());
  }

}
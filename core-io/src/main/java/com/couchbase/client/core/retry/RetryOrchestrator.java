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

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.request.RequestNotRetriedEvent;
import com.couchbase.client.core.cnc.events.request.RequestRetryScheduledEvent;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.UnmonitoredRequest;

import java.time.Duration;
import java.util.Optional;

/**
 * The {@link RetryOrchestrator} is responsible for checking if a request is eligible for retry
 * and if so dispatch it properly and update state.
 *
 * <p>This class has been around since the 1.0 days, but has been adapted to fit the new model
 * where each request can have its own retry strategy.</p>
 *
 * @since 1.0.0
 */
@Stability.Internal
public class RetryOrchestrator {

  /**
   * Retry or cancel the given request, depending on its state and the configured {@link RetryStrategy}.
   *
   * @param ctx the core context into which timer the request is submitted.
   * @param request the request in question.
   * @param reason the reason why the request is being retried.
   */
  public static void maybeRetry(final CoreContext ctx, final Request<? extends Response> request,
                                final RetryReason reason) {
    if (request.completed()) {
      return;
    }

    if (reason.alwaysRetry()) {
      retryWithDuration(ctx, request, controlledBackoff(request.context().retryAttempts()), reason);
      return;
    }

    request.retryStrategy().shouldRetry(request, reason).whenComplete((retryAction, throwable) -> {
      if (throwable != null) {
        ctx.environment().eventBus().publish(
          new RequestNotRetriedEvent(Event.Severity.INFO, request.getClass(), request.context(), reason, throwable)
        );
      }

      Optional<Duration> duration = retryAction.duration();
      if (duration.isPresent()) {
        final Duration cappedDuration = capDuration(duration.get(), request);
        retryWithDuration(ctx, request, cappedDuration, reason);
      } else {
        // unmonitored request's severity is downgraded to debug to not spam the info-level logs
        Event.Severity severity = request instanceof UnmonitoredRequest ? Event.Severity.DEBUG : Event.Severity.INFO;
        ctx.environment().eventBus().publish(
          new RequestNotRetriedEvent(severity, request.getClass(), request.context(), reason, null)
        );
        request.cancel(CancellationReason.noMoreRetries(reason), retryAction.exceptionTranslator());
      }
    });
  }

  /**
   * Calculates the potentially capped retry duration so we do not schedule a longer retry than the actual
   * total timeout.
   *
   * @param uncappedDuration the uncapped proposed duration.
   * @param request the request information.
   * @return the capped duration if needed, otherwise the uncapped duration.
   */
  @Stability.Internal
  public static Duration capDuration(final Duration uncappedDuration, final Request<? extends Response> request) {
    long theoreticalTimeout = System.nanoTime() + uncappedDuration.toNanos();
    long absoluteTimeout = request.absoluteTimeout();
    long timeoutDelta = theoreticalTimeout - absoluteTimeout;
    if (timeoutDelta > 0) {
      Duration cappedDuration = uncappedDuration.minus(Duration.ofNanos(timeoutDelta));
      if (cappedDuration.isNegative()) {
        return uncappedDuration; // something went wrong, return the uncapped one as a safety net
      }
      return cappedDuration;

    }
    return uncappedDuration;
  }

  /**
   * Helper method for a simple, bounded controlled backoff for the "always retry" handler so that it doesn't spam
   * quickly all the time.
   *
   * @param retryAttempt the retry attempts for the request.
   * @return the retry duration.
   */
  @Stability.Internal
  public static Duration controlledBackoff(int retryAttempt) {
    switch (retryAttempt) {
      case 0:
        return Duration.ofMillis(1);
      case 1:
        return Duration.ofMillis(10);
      case 2:
        return Duration.ofMillis(50);
      case 3:
        return Duration.ofMillis(100);
      case 4:
        return Duration.ofMillis(500);
      default:
        return Duration.ofMillis(1000);
    }
  }

  /**
   * Helper method to perform the actual retry with the given duration.
   *
   * @param ctx the core context into which timer the request is submitted.
   * @param request the request in question.
   * @param duration the duration when to retry.
   * @param reason the reason why the request is being retried.
   */
  private static void retryWithDuration(final CoreContext ctx, final Request<? extends Response> request,
                                        final Duration duration, final RetryReason reason) {
    ctx.environment().eventBus().publish(
      new RequestRetryScheduledEvent(duration, request.context(), request.getClass(), reason)
    );
    request.context().incrementRetryAttempts(duration, reason);
    ctx.environment().timer().scheduleForRetry(ctx.core(), request, duration);
  }

}

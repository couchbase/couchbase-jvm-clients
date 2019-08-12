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
import com.couchbase.client.core.cnc.events.request.RequestNotRetriedEvent;
import com.couchbase.client.core.cnc.events.request.RequestRetriedEvent;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;

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
   * Retries the given request immediately, unless it is already completed.
   *
   * <p>This method is usually used in contexts like "not my vbucket" where we need to retry
   * completely transparently and not based on the retry strategy configured.</p>
   *
   * @param ctx the core context into which timer the request is submitted.
   * @param request the request in question.
   * @param reason the reason why the request is being retried.
   */
  public static void retryImmediately(final CoreContext ctx, final Request<? extends Response> request,
                                      final RetryReason reason) {
    if (request.completed()) {
      return;
    }
    retryWithDuration(ctx, request, Duration.ofMillis(1), reason);
  }

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

    Optional<Duration> duration = request.retryStrategy().shouldRetry(request);
    if (duration.isPresent()) {
      retryWithDuration(ctx, request, duration.get(), reason);
    } else {
      ctx.environment().eventBus().publish(new RequestNotRetriedEvent(request.getClass(), request.context()));
      request.cancel(CancellationReason.NO_MORE_RETRIES);
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
      new RequestRetriedEvent(duration, request.context(), request.getClass(), reason)
    );
    request.context().incrementRetryAttempt();
    ctx.environment().timer().schedule(
      () -> ctx.core().send(request,false),
      duration
    );
  }

}

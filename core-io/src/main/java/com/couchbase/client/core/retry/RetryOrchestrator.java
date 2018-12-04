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
   * Retry or cancel the given request, depending on its state and the configured
   * {@link RetryStrategy}.
   *
   * @param ctx the core context into which timer the request is submitted.
   * @param request the request in question.
   */
  public static void maybeRetry(final CoreContext ctx, final Request<? extends Response> request) {
    if (request.completed()) {
      return;
    }

    Optional<Duration> duration = request.retryStrategy().shouldRetry(request);
    if (duration.isPresent()) {
      request.context().incrementRetryAttempt();
      ctx.environment().timer().schedule(
        () -> ctx.core().send(request,false),
        duration.get()
      );
    } else {
      request.cancel(CancellationReason.NO_MORE_RETRIES);
    }
  }

}

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

import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.reactor.Backoff;
import com.couchbase.client.core.retry.reactor.IterationContext;

import java.time.Duration;

public class BestEffortRetryStrategy implements RetryStrategy {

  public static final BestEffortRetryStrategy INSTANCE = BestEffortRetryStrategy
    .withExponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(500), 2);

  private final Backoff backoff;

  private BestEffortRetryStrategy(final Backoff backoff) {
    this.backoff = backoff;
  }

  public static BestEffortRetryStrategy withExponentialBackoff(final Duration lower, final Duration upper,
                                                               final int factor) {
    return new BestEffortRetryStrategy(Backoff.exponential(lower, upper, factor, false));
  }

  @Override
  public RetryAction shouldRetry(final Request<? extends Response> request, final RetryReason reason) {
    if (request.idempotent() || reason.allowsNonIdempotentRetry()) {
      RequestContext ctx = request.context();
      return RetryAction.withDuration(
        backoff.apply(new RetryStrategyIterationContext(ctx.retryAttempts(), ctx.lastRetryDuration())).delay()
      );
    }
    return RetryAction.noRetry();
  }

  @Override
  public String toString() {
    return "BestEffort";
  }

  /**
   * Helper class to make the reactor retry strategies happy we are utilizing internally to avoid code
   * duplication.
   */
  private static class RetryStrategyIterationContext implements IterationContext<Void> {

    private final long iteration;
    private final Duration lastBackoff;

    RetryStrategyIterationContext(long iteration, Duration lastBackoff) {
      this.iteration = iteration;
      this.lastBackoff = lastBackoff;
    }

    @Override
    public Void applicationContext() {
      return null;
    }

    @Override
    public long iteration() {
      return iteration;
    }

    @Override
    public Duration backoff() {
      return lastBackoff;
    }
  }

}

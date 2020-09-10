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
import java.util.concurrent.CompletableFuture;

/**
 * Retries operations on a best-effort basis until they time out.
 * <p>
 * This is the default retry strategy in the SDK and it works under the assumption that all errors which are
 * retryable in the first place will be retried until the operation times out.
 * <p>
 * By default, the time paused between the retries is using an exponential backoff strategy, starting at 1 milliseconds
 * and up to 500 milliseconds (see {@link #DEFAULT_EXPONENTIAL_BACKOFF}). If needed, this backoff can be customized by
 * instantiating a custom instance through the {@link #withExponentialBackoff(Duration, Duration, int)} method.
 * <p>
 * While slightly advanced, this class is designed to be extended and the {@link #shouldRetry(Request, RetryReason)} can
 * be overridden if needed to perform custom retry logic. See the method javadoc for further details.
 */
public class BestEffortRetryStrategy implements RetryStrategy {

  /**
   * The default backoff is an exponential backoff from 1 to 500 millis with a factor of 2.
   */
  private static final Backoff DEFAULT_EXPONENTIAL_BACKOFF = Backoff.exponential(
    Duration.ofMillis(1), Duration.ofMillis(500), 2, false);

  /**
   * Returns the default {@link BestEffortRetryStrategy} instance.
   */
  public static final BestEffortRetryStrategy INSTANCE = new BestEffortRetryStrategy(DEFAULT_EXPONENTIAL_BACKOFF);

  /**
   * Holds the backoff delay algorithm for the retry strategy.
   */
  private final Backoff backoff;

  /**
   * Creates a new {@link BestEffortRetryStrategy} with the {@link #DEFAULT_EXPONENTIAL_BACKOFF}.
   */
  protected BestEffortRetryStrategy() {
    this(DEFAULT_EXPONENTIAL_BACKOFF);
  }

  /**
   * Creates a new {@link BestEffortRetryStrategy} with aa custom {@link Backoff}.
   *
   * @param backoff the custom backoff that should be used.
   */
  protected BestEffortRetryStrategy(final Backoff backoff) {
    this.backoff = backoff;
  }

  /**
   * Creates a new {@link BestEffortRetryStrategy} with custom exponential backoff boundaries.
   *
   * @param lower the lower backoff boundary.
   * @param upper the upper backoff boundary.
   * @param factor the exponential factor to use.
   * @return the instantiated {@link BestEffortRetryStrategy}.
   */
  public static BestEffortRetryStrategy withExponentialBackoff(final Duration lower, final Duration upper,
                                                               final int factor) {
    return new BestEffortRetryStrategy(Backoff.exponential(lower, upper, factor, false));
  }

  /**
   * Determines if a request should be retried or not (and if so, after which duration).
   * <p>
   * In this implementation, the operation will always be retried if it is either idempotent or if the retry reason also
   * allows operations to be retried that are not idempotent. If retry is possible, the next duration based on the
   * configured backoff algorithm is chosen and returned.
   * <p>
   * This method is designed to be overridden by sub-classes. The common use case is that some retry reasons should
   * not be retried while others should be handled as usual. In this case, override this method but make sure to
   * preform custom checks for specific retry reasons (i.e. {@link RetryReason#ENDPOINT_CIRCUIT_OPEN}) but then call
   * this method through super so that all the other reasons are handled properly.
   * <p>
   * While most of the time a {@link CompletableFuture} is constructed immediately, it is possible to call out into
   * external systems through the async mechanism without blocking all the other components in the system. If you do
   * call out over the network or into files, make sure to NEVER block and follow the usual async java/reactor coding
   * best practices.
   *
   * @param request the request that is affected.
   * @param reason the reason why the operation should be retried in the first place.
   * @return a future that when complete indicates the next {@link RetryAction} to take.
   */
  @Override
  public CompletableFuture<RetryAction> shouldRetry(final Request<? extends Response> request, final RetryReason reason) {
    if (request.idempotent() || reason.allowsNonIdempotentRetry()) {
      RequestContext ctx = request.context();
      return CompletableFuture.completedFuture(RetryAction.withDuration(
        backoff.apply(new RetryStrategyIterationContext(ctx.retryAttempts(), ctx.lastRetryDuration())).delay()
      ));
    }
    return CompletableFuture.completedFuture(RetryAction.noRetry());
  }

  @Override
  public String toString() {
    return "BestEffort{backoff=" + backoff + "}";
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

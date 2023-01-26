/*
 * Copyright (c) 2023 Couchbase, Inc.
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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.request.RequestNotRetriedEvent;
import com.couchbase.client.core.cnc.events.request.RequestRetryScheduledEvent;
import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.protostellar.ProtostellarBaseRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.retry.RetryOrchestrator.controlledBackoff;

@Stability.Internal
public class RetryOrchestratorProtostellar {
  private final static Logger logger = LoggerFactory.getLogger(RetryOrchestratorProtostellar.class);

  public static ProtostellarRequestBehaviour shouldRetry(Core core, ProtostellarRequest<?> request, RetryReason reason) {
    CoreContext ctx = core.context();

    if (request.timeoutElapsed()) {
      CancellationErrorContext cancelContext = new CancellationErrorContext(request.context());
      RuntimeException exception = request.idempotent()
        ? new UnambiguousTimeoutException("Request timed out", cancelContext)
        : new AmbiguousTimeoutException("Request timed out", cancelContext);
      return ProtostellarRequestBehaviour.fail(exception);
    }

    if (reason.alwaysRetry()) {
      return retryWithDuration(ctx, request, controlledBackoff(request.retryAttempts()), reason);
    }

    try {
      RetryAction retryAction;

      // The 99% case is that the retry strategy is the default BestEffortRetryStrategy.INSTANCE.  We can avoid turning the ProtostellarRequest into a Request in this case (Request is required by
      // the RetryStrategy public API).
      if (request.retryStrategy() == BestEffortRetryStrategy.INSTANCE) {
        retryAction = RetryAction.withDuration(Duration.ofMillis(50));
      }
      else {
        ProtostellarBaseRequest wrapper = new ProtostellarBaseRequest(core, request);

        retryAction = request.retryStrategy().shouldRetry(wrapper, reason).get();
      }

      Optional<Duration> duration = retryAction.duration();
      if (duration.isPresent()) {
        Duration cappedDuration = capDuration(duration.get(), request);
        return retryWithDuration(ctx, request, cappedDuration, reason);
      } else {
        ctx.environment().eventBus().publish(new RequestNotRetriedEvent(Event.Severity.DEBUG, request.getClass(), request.context(), reason, null));
        return request.cancel(CancellationReason.noMoreRetries(reason));
      }
    }
    catch (Throwable throwable) {
      ctx.environment().eventBus().publish(
        new RequestNotRetriedEvent(Event.Severity.INFO, request.getClass(), request.context(), reason, throwable)
      );
    }

    // If we're retrying we're either going to do that or fail the request due to timeout.
    throw new IllegalStateException("Internal bug - should not reach here");
  }

  private static ProtostellarRequestBehaviour retryWithDuration(final CoreContext ctx, final ProtostellarRequest<?> request,
                                        final Duration duration, final RetryReason reason) {
    Duration cappedDuration = capDuration(duration, request);
    ctx.environment().eventBus().publish(
      new RequestRetryScheduledEvent(cappedDuration, request.context(), request.getClass(), reason)
    );
    request.incrementRetryAttempts(cappedDuration, reason);

    return ProtostellarRequestBehaviour.retry(cappedDuration);
  }

  @Stability.Internal
  public static Duration capDuration(final Duration uncappedDuration, final ProtostellarRequest<?> request) {
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
}

/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.msg;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.retry.RetryStrategy;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Base class for requests sent over HTTP.
 * <p>
 * Adds a cancellation hook, so cancelling the request can also cancel the HTTP call
 * that's sending it (for example, an OkHttp call that's still streaming the response).
 * <p>
 * Not to be confused with the {@link HttpRequest} interface, which describes chunked HTTP requests.
 */
@Stability.Internal
public abstract class BaseHttpRequest<R extends Response> extends BaseRequest<R> {

  /**
   * Runs when the request is cancelled. For example, cancels the HTTP call that's sending it.
   */
  private volatile @Nullable Runnable cancellationHook;

  public BaseHttpRequest(final Duration timeout, final CoreContext ctx, final RetryStrategy retryStrategy) {
    super(timeout, ctx, retryStrategy);
  }

  public BaseHttpRequest(final Duration timeout, final CoreContext ctx, final RetryStrategy retryStrategy,
                         final @Nullable RequestSpan requestSpan) {
    super(timeout, ctx, retryStrategy, requestSpan);
  }

  @Override
  public void cancel(final CancellationReason reason, Function<Throwable, Throwable> exceptionTranslator) {
    super.cancel(reason, exceptionTranslator);

    // Even if this call didn't cancel the request (for example, it already completed),
    // because the HTTP call may still be streaming the response body.
    Runnable hook = cancellationHook;
    if (hook != null) {
      hook.run();
    }
  }

  /**
   * Sets something to run when the request is cancelled (or right away, if it already was).
   * For example, cancelling the HTTP call that's sending the request.
   * <p>
   * The hook may run more than once, so it should be idempotent.
   */
  public void setCancellationHook(Runnable hook) {
    this.cancellationHook = requireNonNull(hook);
    // Check after setting the hook, so a concurrent cancel() runs the hook, or we do (or both).
    if (cancelled()) {
      hook.run();
    }
  }
}

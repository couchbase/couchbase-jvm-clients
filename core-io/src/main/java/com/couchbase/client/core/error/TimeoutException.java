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

package com.couchbase.client.core.error;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.retry.RetryReason;

import java.util.Collections;
import java.util.Set;

/**
 * The {@link TimeoutException} signals that an operation timed out before it could be completed.
 * <p>
 * It is important to understand that the timeout itself is always just the effect an underlying cause, never the
 * issue itself. The root cause might not even be on the application side, also the network and server need to
 * be taken into account.
 * <p>
 * Right now the SDK can throw two different implementations of this class:
 * <ul>
 *   <li>{@link AmbiguousTimeoutException}: The operation might have caused a side effect on the server and should not
 *   be retried without additional actions and checks.</li>
 *   <li>{@link UnambiguousTimeoutException}: The operation has not caused a side effect on the server and is safe
 *   to retry. This is always the case for idempotent operations. For non-idempotent operations it depends on the state
 *   the operation was in at the time of cancellation.</li>
 * </ul>
 * <p>
 * Usually it is helpful to inspect/log the full timeout exception, because it also logs the
 * {@link CancellationErrorContext} which contains lots of additional metadata. The context can also be accessed
 * through the {@link #context()} getter.
 */
public abstract class TimeoutException extends CouchbaseException {

  protected TimeoutException(final String message, final CancellationErrorContext ctx) {
    super(message, ctx);
  }

  @Override
  @Stability.Uncommitted
  public CancellationErrorContext context() {
    return (CancellationErrorContext) super.context();
  }

  /**
   * Returns the set of retry reasons for request.
   *
   * @return the set of retry reasons, might be empty.
   */
  @Stability.Uncommitted
  public Set<RetryReason> retryReasons() {
    final Set<RetryReason> retryReasons = context().requestContext().retryReasons();
    return retryReasons == null ? Collections.emptySet() : retryReasons;
  }

  /**
   * Returns the number of retry attempts for this request.
   *
   * @return the number of retry attempts, might be 0.
   */
  @Stability.Uncommitted
  public int retryAttempts() {
    return context().requestContext().retryAttempts();
  }

}

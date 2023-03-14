/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.java.authentication.fastfail;

import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryAction;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.retry.RetryStrategy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A custom {@link RetryStrategy} that behaves identically to the default {@link BestEffortRetryStrategy}
 * except after receiving 'N' AUTH_ERRORs, which will fast-fail the operation with an {@link AuthenticationFailureException}.
 * <p>
 * This must be created on a per-operation basis.
 */
public class FastFailOnNthAuthErrorRetryStrategy implements RetryStrategy {
  private final AtomicInteger authErrors = new AtomicInteger();
  private final int requiredAuthErrors;

  public FastFailOnNthAuthErrorRetryStrategy(int requiredAuthErrors) {
    this.requiredAuthErrors = requiredAuthErrors;
  }

  /**
   * For testing purposes, the number of auth errors seen.
   */
  public int authErrors() {
    return authErrors.get();
  }

  @Override
  public CompletableFuture<RetryAction> shouldRetry(Request<? extends Response> request, RetryReason reason) {
    if (reason == RetryReason.AUTHENTICATION_ERROR && authErrors.incrementAndGet() >= requiredAuthErrors) {
      return CompletableFuture.completedFuture(
        RetryAction.noRetry((err) -> AuthenticationFailureException.onAuthError(request, err)));
    }

    return BestEffortRetryStrategy.INSTANCE.shouldRetry(request, reason);
  }
}

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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;

import java.util.concurrent.CompletableFuture;

/**
 * An internal strategy to fail fast any request that goes through it.
 * <p>
 * This class is considered internal! For external purposes we recommend extending the {@link BestEffortRetryStrategy}
 * and only failing on certain {@link RetryReason} as needed or based on a request context or external property. Using
 * this strategy here directly, if not used carefully, will lead to lot of copy/pasting of try/catch blocks and not
 * to clean code.
 */
@Stability.Internal
public class FailFastRetryStrategy implements RetryStrategy {

  public static final FailFastRetryStrategy INSTANCE = new FailFastRetryStrategy();

  private FailFastRetryStrategy() { }

  @Override
  public CompletableFuture<RetryAction> shouldRetry(Request<? extends Response> request, RetryReason reason) {
    return CompletableFuture.completedFuture(RetryAction.noRetry());
  }

  @Override
  public String toString() {
    return "FailFast";
  }
}

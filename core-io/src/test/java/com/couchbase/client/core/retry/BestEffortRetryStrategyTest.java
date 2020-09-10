/*
 * Copyright (c) 2020 Couchbase, Inc.
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
import com.couchbase.client.core.msg.Response;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the functionality of the {@link BestEffortRetryStrategy}.
 */
class BestEffortRetryStrategyTest {

  @Test
  void canBeOverridden() throws Exception {
    RetryStrategy customStrategy = new BestEffortRetryStrategy() {
      @Override
      public CompletableFuture<RetryAction> shouldRetry(Request<? extends Response> request, RetryReason reason) {
        if (reason == RetryReason.ENDPOINT_CIRCUIT_OPEN) {
          return CompletableFuture.completedFuture(RetryAction.noRetry());
        }
        return super.shouldRetry(request, reason);
      }
    };

    RetryAction retryAction = customStrategy.shouldRetry(null, RetryReason.ENDPOINT_CIRCUIT_OPEN).get();
    assertEquals(RetryAction.noRetry(), retryAction);
  }

}
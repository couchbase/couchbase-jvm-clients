/*
 * Copyright 2022 Couchbase, Inc.
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

package com.couchbase.client.java;

import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryAction;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertThrows;

@IgnoreWhen(isProtostellarWillWorkLater = true)
class RetryStrategyIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static Collection collection;

  @BeforeAll
  static void setup() {
    cluster = createCluster();
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  private static class CustomDocumentLockedException extends RuntimeException {
    public CustomDocumentLockedException(Throwable cause) {
      super(cause);
    }
  }

  private static final RetryStrategy failFastIfLocked = new BestEffortRetryStrategy() {
    @Override
    public CompletableFuture<RetryAction> shouldRetry(final Request<? extends Response> request, final RetryReason reason) {
      return reason == RetryReason.KV_LOCKED
          ? completedFuture(RetryAction.noRetry(CustomDocumentLockedException::new))
          : super.shouldRetry(request, reason);
    }
  };

  @Test
  void retryStrategyCanTriggerCustomException() {
    String docId = UUID.randomUUID().toString();

    collection.upsert(docId, "foo");
    GetResult r = collection.getAndLock(docId, Duration.ofSeconds(15));

    assertThrows(CustomDocumentLockedException.class, () ->
        collection.remove(docId, removeOptions()
            .retryStrategy(failFastIfLocked)));

    collection.unlock(docId, r.cas());
    collection.remove(docId);
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }
}

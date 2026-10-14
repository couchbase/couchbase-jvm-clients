/*
 * Copyright (c) 2026 Couchbase, Inc.
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

package com.couchbase.client.core;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.GetAndLockRequest;
import com.couchbase.client.core.msg.kv.GetAndLockResponse;
import com.couchbase.client.core.msg.kv.RemoveRequest;
import com.couchbase.client.core.msg.kv.UnlockRequest;
import com.couchbase.client.core.msg.kv.UpsertRequest;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryAction;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.util.CoreIntegrationTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that shutting down the core and its environment completes requests
 * that are waiting in retry backoff, instead of leaving them hanging (JVMCBC-1786).
 */
class ShutdownDuringRetryIntegrationTest extends CoreIntegrationTest {

  private static Core core;
  private static CoreEnvironment env;

  /**
   * Retries a locked document only after a long delay, so the request is reliably
   * still waiting in the timer when the environment is shut down.
   */
  private static final RetryStrategy longBackoffIfLocked = new BestEffortRetryStrategy() {
    @Override
    public CompletableFuture<RetryAction> shouldRetry(final Request<? extends Response> request, final RetryReason reason) {
      return reason == RetryReason.KV_LOCKED
        ? completedFuture(RetryAction.withDuration(Duration.ofMinutes(1)))
        : super.shouldRetry(request, reason);
    }
  };

  @BeforeAll
  static void beforeAll() {
    env = environment().build();
    core = Core.create(env, authenticator(), seedNodes());
    core.openBucket(config().bucketname());
    waitUntilReady(core);
  }

  @AfterAll
  static void afterAll() {
    core.shutdown().block();
    env.shutdown();
  }

  @Test
  void shutdownCancelsRequestWaitingForRetry() throws Exception {
    String id = UUID.randomUUID().toString();
    CollectionIdentifier collection = CollectionIdentifier.fromDefault(config().bucketname());

    UpsertRequest upsertRequest = new UpsertRequest(id, "foo".getBytes(UTF_8), 0, false, 0, kvTimeout,
      core.context(), collection, env.retryStrategy(), Optional.empty(), null);
    core.send(upsertRequest);
    assertTrue(upsertRequest.response().get().status().success());

    GetAndLockRequest lockRequest = new GetAndLockRequest(id, kvTimeout, core.context(), collection,
      env.retryStrategy(), Duration.ofSeconds(15), null);
    core.send(lockRequest);
    GetAndLockResponse lockResponse = lockRequest.response().get();
    assertTrue(lockResponse.status().success());

    try {
      CoreEnvironment otherEnv = environment().build();
      Core otherCore = Core.create(otherEnv, authenticator(), seedNodes());
      otherCore.openBucket(config().bucketname());
      waitUntilReady(otherCore);

      RemoveRequest removeRequest = new RemoveRequest(id, 0, Duration.ofMinutes(2), otherCore.context(),
        collection, longBackoffIfLocked, Optional.empty(), null);
      otherCore.send(removeRequest);

      // Wait until the remove is sleeping in retry backoff.
      waitUntilCondition(() -> removeRequest.context().retryAttempts() > 0);

      // Same sequence as Cluster.disconnect() with an SDK-owned environment.
      otherCore.shutdown().block();
      otherEnv.shutdown();

      // Before the fix, the response never completed.
      ExecutionException e = assertThrows(ExecutionException.class,
        () -> removeRequest.response().get(10, TimeUnit.SECONDS));
      assertInstanceOf(RequestCanceledException.class, e.getCause());
      assertEquals(CancellationReason.SHUTDOWN, removeRequest.cancellationReason());

    } finally {
      UnlockRequest unlockRequest = new UnlockRequest(kvTimeout, core.context(), collection,
        env.retryStrategy(), id, lockResponse.cas(), null);
      core.send(unlockRequest);
      unlockRequest.response().get();

      RemoveRequest cleanupRequest = new RemoveRequest(id, 0, kvTimeout, core.context(), collection,
        env.retryStrategy(), Optional.empty(), null);
      core.send(cleanupRequest);
      cleanupRequest.response().get();
    }
  }
}

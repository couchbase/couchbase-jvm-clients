/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.endpoint.CircuitBreakerConfig;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.GetResponse;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.util.JavaIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies the default behavior of the circuit breaker when enabled.
 */
class CircuitBreakerIntegrationTest extends JavaIntegrationTest {

  private Cluster cluster;
  private Collection collection;

  @BeforeEach
  void beforeEach() {
    cluster = createCluster(env -> env.ioConfig(IoConfig.kvCircuitBreakerConfig(CircuitBreakerConfig.enabled(true))));
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();
    bucket.waitUntilReady(Duration.ofSeconds(5));
  }

  @AfterEach
  void afterEach() {
    cluster.disconnect();
  }

  /**
   * We need to make sure that other exceptions (like key not found) do not trigger the fail fast mechanism
   * of the circuit breaker, only those that are actually defined should open the circuit.
   */
  @Test
  void shouldNotTriggerWhenNotTimeouts() {
    int threshold = collection.environment().ioConfig().kvCircuitBreakerConfig().volumeThreshold();
    for (int i = 0; i < threshold * 3; i++) {
      assertThrows(DocumentNotFoundException.class, () -> collection.get("this-doc-does-not-exist"));
    }
  }

  @Test
  void shouldTriggerWithTimeouts() {
    int threshold = collection.environment().ioConfig().kvCircuitBreakerConfig().volumeThreshold();

    int timeouts = 0;
    int cancellations = 0;
    for (int i = 0; i < threshold * 3; i++) {
      FailingGetRequestOnEncode request = new FailingGetRequestOnEncode(
        "foo",
        Duration.ofMillis(1),
        collection.core().context(),
        new CollectionIdentifier(
          config().bucketname(),
          Optional.of(collection.scopeName()),
          Optional.of(collection.name())
        ),
        FailFastRetryStrategy.INSTANCE
      );
      collection.core().send(request);


      try {
        request.response().get();
        fail();
      } catch (ExecutionException ex) {
        if (ex.getCause() instanceof TimeoutException) {
          timeouts++;
        } else if (ex.getCause() instanceof RequestCanceledException) {
          cancellations++;
          CancellationReason reason = ((RequestCanceledException) ex.getCause()).context().requestContext().request().cancellationReason();
          assertEquals(reason.innerReason(), RetryReason.ENDPOINT_CIRCUIT_OPEN);
        }
      } catch (Throwable t) {
        fail(t);
      }
    }

    assertTrue(timeouts > 0);
    assertTrue(cancellations > 0);
  }

  private static class FailingGetRequestOnEncode extends GetRequest {
    FailingGetRequestOnEncode(String key, Duration timeout, CoreContext ctx,
                              CollectionIdentifier collectionIdentifier, RetryStrategy retryStrategy) {
      super(key, timeout, ctx, collectionIdentifier, retryStrategy, null);
    }

    @Override
    public GetResponse decode(ByteBuf response, KeyValueChannelContext ctx) {
      this.cancel(CancellationReason.TIMEOUT);
      return super.decode(response, ctx);
    }
  }

}

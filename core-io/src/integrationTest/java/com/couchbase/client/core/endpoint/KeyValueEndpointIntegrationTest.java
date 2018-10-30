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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.msg.kv.NoopRequest;
import com.couchbase.client.core.msg.kv.NoopResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.util.ClusterAwareIntegrationTest;
import com.couchbase.client.util.TestNodeConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.util.Utils.waitUntilCondition;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the basic interaction between the {@link KeyValueEndpoint} and a
 * single node in a very basic but still end-to-end fashion.
 *
 * @since 2.0.0
 */
class KeyValueEndpointIntegrationTest extends ClusterAwareIntegrationTest {

  private CoreEnvironment env;
  private CoreContext coreContext;

  @BeforeEach
  void beforeEach() {
    env = CoreEnvironment.create();
    coreContext = new CoreContext(1, env);
  }

  @AfterEach
  void afterEach() {
    env.shutdown(Duration.ofSeconds(1));
  }

  /**
   * The most simplistic end-to-end test for a KV endpoint.
   *
   * <p>This integration test connects to a node and then performs a NOOP and
   * waits for a successful response.</p>
   *
   * @throws Exception if waiting on the response fails.
   */
  @Test
  void connectNoopAndDisconnect() throws Exception {
    TestNodeConfig node = config().nodes().get(0);

    KeyValueEndpoint endpoint = new KeyValueEndpoint(
      coreContext,
      NetworkAddress.create(node.hostname()),
      node.ports().get(ServiceType.KV),
      config().adminUsername(),
      config().bucketname(),
      config().adminPassword()
    );

    endpoint.connect();
    waitUntilCondition(() -> endpoint.state() == EndpointState.CONNECTED_CIRCUIT_CLOSED);

    NoopRequest request = new NoopRequest(Duration.ZERO, null);
    endpoint.send(request);

    NoopResponse response = request.response().get(1, TimeUnit.SECONDS);
    assertTrue(response.status().success());

    endpoint.disconnect();
    waitUntilCondition(() -> endpoint.state() == EndpointState.DISCONNECTED);
  }

}

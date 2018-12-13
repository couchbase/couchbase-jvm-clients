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
import com.couchbase.client.core.msg.manager.TerseBucketConfigRequest;
import com.couchbase.client.core.msg.manager.TerseBucketConfigResponse;
import com.couchbase.client.test.ClusterAwareIntegrationTest;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.Assert.assertTrue;

class ManagerEndpointIntegrationTest extends ClusterAwareIntegrationTest {

  private CoreEnvironment env;
  private CoreContext coreContext;

  @BeforeEach
  void beforeEach() {
    env = CoreEnvironment.create(config().adminUsername(), config().adminPassword());
    coreContext = new CoreContext(null, 1, env);
  }

  @AfterEach
  void afterEach() {
    env.shutdown(Duration.ofSeconds(1));
  }

  /**
   * This integration test attempts to load a "terse" bucket config from the cluster manager.
   *
   * <p>Note that the actual response is not checked here, since this handles at the higher levels. We just make sure
   * that the config returned is not empty.</p>
   */
  @Test
  void fetchTerseConfig() throws Exception {
    TestNodeConfig node = config().nodes().get(0);

    ManagerEndpoint endpoint = new ManagerEndpoint(
      coreContext,
      NetworkAddress.create(node.hostname()),
      node.ports().get(Services.MANAGER)
    );

    endpoint.connect();
    waitUntilCondition(() -> endpoint.state() == EndpointState.CONNECTED);

    TerseBucketConfigRequest request = new TerseBucketConfigRequest(Duration.ofSeconds(1), coreContext, null,
      config().bucketname(), env.credentials(), null);

    assertTrue(request.id() > 0);
    endpoint.send(request);

    TerseBucketConfigResponse response = request.response().get(1, TimeUnit.SECONDS);
    assertTrue(response.status().success());
    assertTrue(response.config().length > 0);

    endpoint.disconnect();
    waitUntilCondition(() -> endpoint.state() == EndpointState.DISCONNECTED);
  }

}

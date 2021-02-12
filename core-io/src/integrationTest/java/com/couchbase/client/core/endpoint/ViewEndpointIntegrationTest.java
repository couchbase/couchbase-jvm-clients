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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.view.GenericViewRequest;
import com.couchbase.client.core.msg.view.GenericViewResponse;
import com.couchbase.client.core.service.ServiceContext;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies basic dispatch functionality of the view endpoint.
 */
@IgnoreWhen(missesCapabilities = Capabilities.VIEWS)
class ViewEndpointIntegrationTest extends CoreIntegrationTest {

  private static CoreEnvironment env;
  private static ServiceContext serviceContext;

  @BeforeAll
  static void beforeAll() {
    TestNodeConfig node = config().nodes().get(0);

    env = environment().ioConfig(IoConfig.captureTraffic(ServiceType.VIEWS)).build();
    serviceContext = new ServiceContext(
      new CoreContext(null, 1, env, authenticator()),
      node.hostname(),
      node.ports().get(Services.VIEW),
      ServiceType.VIEWS,
      Optional.empty()
    );
  }

  @AfterAll
  static void afterAll() {
    env.shutdown();
  }

  /**
   * Makes sure that we can execute a generic view management request.
   *
   * <p>The mock does not support hitting the / path for views, so this test is ignored there.</p>
   */
  @Test
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED)
  void dispatchGenericRequest() throws Exception {
    TestNodeConfig node = config().nodes().get(0);

    ViewEndpoint endpoint = new ViewEndpoint(
      serviceContext,
      node.hostname(),
      node.ports().get(Services.VIEW)
    );

    endpoint.connect();
    waitUntilCondition(() -> endpoint.state() == EndpointState.CONNECTED);

    GenericViewRequest request = new GenericViewRequest(
      Duration.ofSeconds(5),
      serviceContext,
      env.retryStrategy(),
      () -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"),
      true,
      config().bucketname(),
      null
    );
    endpoint.send(request);

    GenericViewResponse response = request.response().get();
    assertEquals(ResponseStatus.SUCCESS, response.status());
    assertNotNull(response.content());
    assertTrue(response.content().length > 0);

    endpoint.disconnect();
    waitUntilCondition(() -> endpoint.state() == EndpointState.DISCONNECTED);
  }

}

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
package com.couchbase.client.core;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@IgnoreWhen(missesCapabilities = {Capabilities.GLOBAL_CONFIG, Capabilities.QUERY})
class ClusterLevelQueryIntegrationTest extends CoreIntegrationTest {

  private Core core;
  private CoreEnvironment env;

  @BeforeEach
  void beforeEach() {
    env = environment().build();
    core = Core.create(env, authenticator());

    core.initGlobalConfig().block();
  }

  @AfterEach
  void afterEach() {
    core.shutdown().block();
    env.shutdown();
  }

  @Test
  void performsClusterLevelQueryWithoutOpenBucket() throws Exception {
    String query = "{\"statement\": \"select 1=1\"}";

    QueryRequest request = new QueryRequest(
      env.timeoutConfig().queryTimeout(),
      core.context(),
      env.retryStrategy(),
      core.context().authenticator(),
      "select 1=1",
      query.getBytes(StandardCharsets.UTF_8),
      true
    );
    core.send(request);

    QueryResponse response = request.response().get();
    assertNotNull(response.header().requestId());

    List<QueryChunkRow> rows = response.rows().collectList().block();
    assertNotNull(rows);
    assertEquals(1, rows.size());
  }

}

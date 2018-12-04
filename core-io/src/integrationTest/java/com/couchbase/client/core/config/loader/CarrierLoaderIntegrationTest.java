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

package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.util.ClusterAwareIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

class CarrierLoaderIntegrationTest extends ClusterAwareIntegrationTest {

  private CoreEnvironment env;

  @BeforeEach
  void beforeEach() {
    env = CoreEnvironment.create(config().adminUsername(), config().adminPassword());
  }

  @AfterEach
  void afterEach() {
    env.shutdown(Duration.ofSeconds(1));
  }

  /**
   * This is a very simplistic test that makes sure that we can "round trip" in the
   * {@link CarrierLoader} by grabbing a JSON decodable config through the full stack.
   */
  @Test
  void loadConfigViaCarrierPublication() {
    NetworkAddress hostname = NetworkAddress.create(config().nodes().get(0).hostname());

    Core core = Core.create(env);
    CarrierLoader loader = new CarrierLoader(core);
    BucketConfig loaded = loader.load(hostname, config().bucketname()).block();

    assertNotNull(loaded);
    assertEquals(config().bucketname(), loaded.name());
  }

}

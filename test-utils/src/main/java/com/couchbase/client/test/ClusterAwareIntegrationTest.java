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
package com.couchbase.client.test;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

/**
 * Parent class which drives all dynamic integration tests based on the configured
 * cluster setup.
 *
 * @since 2.0.0
 */
@ExtendWith(ClusterInvocationProvider.class)
// Sanity timer that should be sufficient for majority of tests.
@Timeout(120)
public class ClusterAwareIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterAwareIntegrationTest.class);

  static {
    try {
      BlockHound
        .builder()
        .with(builder -> {
          builder
            .allowBlockingCallsInside(Util.class.getName(), "waitUntilCondition")
            .allowBlockingCallsInside(Util.class.getName(), "waitUntilThrows")
            .allowBlockingCallsInside("reactor.core.scheduler.ParallelScheduler", "dispose");
         })
        // We need to do a custom callback, since the default one seems to be stuck in tests for some reason
        .blockingMethodCallback(blockingMethod -> LOGGER.warn("Blocking Method Detected: " + blockingMethod))
        .install();
    } catch (Exception ex) {
      // For now, silently ignore not running
      // with blockhound enabled, see
      // https://github.com/reactor/BlockHound/issues/33
    }
  }

  private static TestClusterConfig testClusterConfig;

  @BeforeAll
  static void setup(TestClusterConfig config) {
    testClusterConfig = config;
  }

  /**
   * Returns the current config for the integration test cluster.
   */
  public static TestClusterConfig config() {
    return testClusterConfig;
  }


}

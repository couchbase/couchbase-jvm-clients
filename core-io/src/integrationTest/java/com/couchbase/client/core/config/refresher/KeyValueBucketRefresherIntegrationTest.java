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

package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.Util;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Verifies the functionality of the {@link KeyValueBucketRefresher}.
 *
 * <p>Note that the unit test covers the different error cases. In here we just make sure
 * that configs are loaded in the "good" cases.</p>
 */
class KeyValueBucketRefresherIntegrationTest extends CoreIntegrationTest {

  private CoreEnvironment env;

  /**
   * We are using a shorter config poll interval in this test to keep the test runtime
   * small.
   */
  @BeforeEach
  void beforeEach() {
    env = environment()
      .ioConfig(IoConfig.configPollInterval(Duration.ofMillis(100)))
      .build();
  }

  @AfterEach
  void afterEach() {
    env.shutdown();
  }

  @Test
  void pollsForNewConfigs() {
    Core core = Core.create(env, authenticator(), seedNodes());

    ProposedBucketConfigInspectingProvider inspectingProvider =
      new ProposedBucketConfigInspectingProvider(core.configurationProvider());
    KeyValueBucketRefresher refresher = new KeyValueBucketRefresher(inspectingProvider, core) {
      @Override
      protected Duration pollerInterval() {
        return Duration.ofMillis(10); // fire quickly to speed up the integration test.
      }
    };

    core.openBucket(config().bucketname()).block();
    refresher.register(config().bucketname()).block();
    long expected = env.ioConfig().configPollInterval().toNanos();

    Util.waitUntilCondition(() -> {
      List<Long> timings = new ArrayList<>(inspectingProvider.proposedTimings());
      int size = timings.size();
      if (size < 2) {
        return false; // we need at least 2 records to compare
      }
      return (timings.get(size - 1) - timings.get(size - 2)) >= expected;
    });

    refresher.deregister(config().bucketname()).block();

    long size = inspectingProvider.proposedTimings().size();
    Util.waitUntilCondition(() -> inspectingProvider.proposedTimings().size() == size);

    for (ProposedBucketConfigContext config : inspectingProvider.proposedConfigs()) {
      assertEquals(config().bucketname(), config.bucketName());
      assertNotNull(config.config());
    }

    refresher.shutdown().block();
    inspectingProvider.shutdown().block();
  }

}

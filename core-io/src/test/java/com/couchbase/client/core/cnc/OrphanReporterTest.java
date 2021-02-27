/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.core.cnc;

import com.couchbase.client.core.env.OrphanReporterConfig;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies basic functionality of the {@link OrphanReporter}.
 */
class OrphanReporterTest {

  @Test
  void enabledReporter() throws Exception {
    OrphanReporter reporter = newOrphanReporter(
        OrphanReporterConfig.enabled(true));

    reporter.start().block();
    assertNotNull(reporter.worker);
    assertTrue(reporter.worker.isAlive());

    reporter.stop().block();
    reporter.worker.join(SECONDS.toMillis(30));
    assertFalse(reporter.worker.isAlive());
  }

  @Test
  void disabledReporter() {
    OrphanReporter reporter = newOrphanReporter(
        OrphanReporterConfig.enabled(false));

    reporter.start().block();
    assertNull(reporter.worker);

    // stopping should not throw an exception
    reporter.stop().block();
  }

  private static OrphanReporter newOrphanReporter(OrphanReporterConfig.Builder builder) {
    return new OrphanReporter(new SimpleEventBus(false), builder.build());
  }
}

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

import java.util.Set;

import org.junit.jupiter.api.Test;

import static com.couchbase.client.core.cnc.OrphanReporter.ORPHAN_TREAD_PREFIX;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies basic functionality of the {@link OrphanReporter}.
 */
class OrphanReporterTest {

  @Test
  void enabledReporter() {
    OrphanReporter reporter = new OrphanReporter(
        new SimpleEventBus(false),
        OrphanReporterConfig.enabled(true).
        build()
    );

    reporter.start().block();

    Set<Thread> threads = Thread.getAllStackTraces().keySet();

    for (Thread thread : threads) {
      if (thread.getName().startsWith(ORPHAN_TREAD_PREFIX)) {
        assertTrue(true);
        reporter.stop().block();
        return;
      }
    }
    reporter.stop().block();
    fail();
  }

  @Test
  void disabledReporter() {
    OrphanReporter reporter = new OrphanReporter(
        new SimpleEventBus(false),
        OrphanReporterConfig.enabled(false).
        build()
    );

    reporter.start().block();

    Set<Thread> threads = Thread.getAllStackTraces().keySet();

    for (Thread thread : threads) {
      if (thread.getName().startsWith(ORPHAN_TREAD_PREFIX)) {
        fail();
        reporter.stop().block();
        return;
      }
    }
    reporter.stop().block();
  }
}

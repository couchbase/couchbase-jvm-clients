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

package com.couchbase.client.core.cnc;

import com.couchbase.client.core.cnc.events.diagnostics.GarbageCollectionDetectedEvent;
import com.couchbase.client.util.SimpleEventBus;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Supplier;

import static com.couchbase.client.util.Utils.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Verifies the functionality of the {@link DiagnosticsMonitor}.
 *
 * @since 2.0.0
 */
class DiagnosticsMonitorTest {

  @Test
  void capturesGcEvents() {
    assumeTrue(explicitGcEnabled(), "-XX:+DisableExplicitGC set");

    SimpleEventBus eventBus = new SimpleEventBus(false);
    DiagnosticsMonitor monitor = DiagnosticsMonitor.create(eventBus);
    monitor.start().block();

    Supplier<Optional<GarbageCollectionDetectedEvent>> s = () -> new ArrayList<>(eventBus.publishedEvents())
      .stream()
      .filter(event -> event instanceof GarbageCollectionDetectedEvent)
      .map(event -> (GarbageCollectionDetectedEvent) event)
      .filter(event -> event.cause().equals("System.gc()"))
      .findFirst();

    System.gc();
    waitUntilCondition(() -> s.get().isPresent());

    GarbageCollectionDetectedEvent event = s.get().get();
    assertEquals("System.gc()", event.cause());
    assertTrue(event.memoryBefore() != 0 || event.memoryAfter() != 0);
    assertTrue(event.duration().toNanos() > 0);

    monitor.stop().block();
  }

  /**
   * Helper method to check if explicit GC is not disabled via flags.
   *
   * @return true if it enabled, false otherwise.
   */
  private static boolean explicitGcEnabled() {
    return ManagementFactory
      .getRuntimeMXBean()
      .getInputArguments()
      .stream()
      .noneMatch(s -> s.contains("DisableExplicitGC"));
  }

}
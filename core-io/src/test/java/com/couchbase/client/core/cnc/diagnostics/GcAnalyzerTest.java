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

package com.couchbase.client.core.cnc.diagnostics;

import com.couchbase.client.core.cnc.events.diagnostics.GarbageCollectionDetectedEvent;
import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import org.junit.jupiter.api.Test;

import java.lang.management.MemoryUsage;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct emission of events based on GC info notifications.
 *
 * @since 2.0.0
 */
class GcAnalyzerTest {

  /**
   * This test makes sure that a PS Scavenge (young gen, stop the world) event is
   * properly converted.
   */
  @Test
  void convertPSScavenge() {
    GarbageCollectionDetectedEvent event = new GcAnalyzer("PS Scavenge")
      .apply(new InfoBuilder()
        .action("end of minor GC")
        .cause("System.gc()")
        .duration(5)
        .poolBefore("PS Eden Space", 21497256)
        .poolAfter("PS Eden Space", 1)
        .poolBefore("PS Survivor Space", 1)
        .poolAfter("PS Survivor Space", 2704608)
        .build()
    );

    assertEquals(21497257, event.memoryBefore());
    assertEquals(2704609, event.memoryAfter());
    assertEquals(Duration.ofMillis(5), event.duration());
    assertEquals(GcAnalyzer.GcType.PS_SCAVENGE, event.type());
    assertEquals("System.gc()", event.cause());
    assertEquals("end of minor GC", event.action());
  }

  @Test
  void convertPSMarkSweep() {
    GarbageCollectionDetectedEvent event = new GcAnalyzer("PS MarkSweep")
      .apply(new InfoBuilder()
        .action("end of major GC")
        .cause("System.gc()")
        .duration(200)
        .poolBefore("PS Old Gen", 16384)
        .poolAfter("PS Old Gen", 2446680)
        .build()
      );

    assertEquals(16384, event.memoryBefore());
    assertEquals(2446680, event.memoryAfter());
    assertEquals(Duration.ofMillis(200), event.duration());
    assertEquals(GcAnalyzer.GcType.PS_MARK_SWEEP, event.type());
    assertEquals("System.gc()", event.cause());
    assertEquals("end of major GC", event.action());
  }

  @Test
  void convertCopy() {
    GarbageCollectionDetectedEvent event = new GcAnalyzer("Copy")
      .apply(new InfoBuilder()
        .action("end of minor GC")
        .cause("System.gc()")
        .duration(5)
        .poolBefore("Eden Space", 21497256)
        .poolAfter("Eden Space", 1)
        .poolBefore("Survivor Space", 1)
        .poolAfter("Survivor Space", 2704608)
        .build()
      );

    assertEquals(21497257, event.memoryBefore());
    assertEquals(2704609, event.memoryAfter());
    assertEquals(Duration.ofMillis(5), event.duration());
    assertEquals(GcAnalyzer.GcType.COPY, event.type());
    assertEquals("System.gc()", event.cause());
    assertEquals("end of minor GC", event.action());
  }

  @Test
  void convertMarkSweepCompact() {
    GarbageCollectionDetectedEvent event = new GcAnalyzer("MarkSweepCompact")
      .apply(new InfoBuilder()
        .action("end of major GC")
        .cause("System.gc()")
        .duration(200)
        .poolBefore("Tenured Gen", 16384)
        .poolAfter("Tenured Gen", 2446680)
        .build()
      );

    assertEquals(16384, event.memoryBefore());
    assertEquals(2446680, event.memoryAfter());
    assertEquals(Duration.ofMillis(200), event.duration());
    assertEquals(GcAnalyzer.GcType.MARK_SWEEP_COMPACT, event.type());
    assertEquals("System.gc()", event.cause());
    assertEquals("end of major GC", event.action());
  }

  // todo: add tests for g1 young and g1 old
  // todo: add par new
  // todo: add CMS

  class InfoBuilder {

    private String action;
    private String cause;
    private long duration;
    private final Map<String, MemoryUsage> poolBefore = new HashMap<>();
    private final Map<String, MemoryUsage> poolAfter = new HashMap<>();

    InfoBuilder action(String action) {
      this.action = action;
      return this;
    }

    InfoBuilder cause(String cause) {
      this.cause = cause;
      return this;
    }

    InfoBuilder duration(long duration) {
      this.duration = duration;
      return this;
    }

    InfoBuilder poolBefore(String key, long value) {
      MemoryUsage usage = mock(MemoryUsage.class);
      when(usage.getUsed()).thenReturn(value);
      poolBefore.put(key, usage);
      return this;
    }

    InfoBuilder poolAfter(String key, long value) {
      MemoryUsage usage = mock(MemoryUsage.class);
      when(usage.getUsed()).thenReturn(value);
      poolAfter.put(key, usage);
      return this;
    }

    GarbageCollectionNotificationInfo build() {
      GarbageCollectionNotificationInfo fullInfo = mock(GarbageCollectionNotificationInfo.class);
      when(fullInfo.getGcAction()).thenReturn(action);
      when(fullInfo.getGcCause()).thenReturn(cause);

      GcInfo gcInfo = mock(GcInfo.class);
      when(fullInfo.getGcInfo()).thenReturn(gcInfo);
      when(gcInfo.getDuration()).thenReturn(duration);
      when(gcInfo.getMemoryUsageBeforeGc()).thenReturn(poolBefore);
      when(gcInfo.getMemoryUsageAfterGc()).thenReturn(poolAfter);
      return fullInfo;
    }
  }
}
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

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.diagnostics.GarbageCollectionsDetectedEvent;
import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct emission of events based on GC info notifications.
 *
 * @since 2.0.0
 */
class GcAnalyzerTest {

  @Test
  void handlesKnownGC() {
    GcAnalyzer analyzer = new GcAnalyzer();

    analyzer.handleNotification(new InfoBuilder().duration(5).build("PS Scavenge"));
    analyzer.handleNotification(new InfoBuilder().duration(50).build("PS Scavenge"));
    analyzer.handleNotification(new InfoBuilder().duration(500).build("PS Scavenge"));
    analyzer.handleNotification(new InfoBuilder().duration(5000).build("PS Scavenge"));

    GarbageCollectionsDetectedEvent ev = analyzer.fetchEvent(Event.Severity.DEBUG, null);
    assertEquals(5000, ev.maxValue(GcAnalyzer.GcType.PS_SCAVENGE));
    assertEquals(50, ev.valueAtPercentile(GcAnalyzer.GcType.PS_SCAVENGE, 50.0));
    assertEquals(1, ev.types().size());
    assertEquals(GcAnalyzer.GcType.PS_SCAVENGE, ev.types().iterator().next());
  }

  @Test
  void handlesUnknownGC() {
    GcAnalyzer analyzer = new GcAnalyzer();

    analyzer.handleNotification(new InfoBuilder().duration(5).build("That New GC"));
    analyzer.handleNotification(new InfoBuilder().duration(50).build("That New GC"));
    analyzer.handleNotification(new InfoBuilder().duration(500).build("That New GC"));
    analyzer.handleNotification(new InfoBuilder().duration(5000).build("That New GC"));

    GarbageCollectionsDetectedEvent ev = analyzer.fetchEvent(Event.Severity.DEBUG, null);
    assertEquals(5000, ev.maxValue(GcAnalyzer.GcType.UNKNOWN));
    assertEquals(50, ev.valueAtPercentile(GcAnalyzer.GcType.UNKNOWN, 50.0));
    assertEquals(1, ev.types().size());
    assertEquals(GcAnalyzer.GcType.UNKNOWN, ev.types().iterator().next());
  }

  class InfoBuilder {

    private long duration;

    InfoBuilder duration(long duration) {
      this.duration = duration;
      return this;
    }

    GarbageCollectionNotificationInfo build(String name) {
      GarbageCollectionNotificationInfo fullInfo = mock(GarbageCollectionNotificationInfo.class);

      GcInfo gcInfo = mock(GcInfo.class);
      when(fullInfo.getGcInfo()).thenReturn(gcInfo);
      when(fullInfo.getGcName()).thenReturn(name);
      when(gcInfo.getDuration()).thenReturn(duration);

      return fullInfo;
    }
  }

}
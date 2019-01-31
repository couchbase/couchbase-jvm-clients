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
import com.couchbase.client.core.cnc.events.diagnostics.PausesDetectedEvent;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PauseAnalyzerTest {

  @Test
  void recordsValues() {
    PauseAnalyzer analyzer = new PauseAnalyzer();

    analyzer.handlePauseEvent(TimeUnit.MILLISECONDS.toNanos(10), System.nanoTime());
    analyzer.handlePauseEvent(TimeUnit.MILLISECONDS.toNanos(100), System.nanoTime());
    analyzer.handlePauseEvent(TimeUnit.MILLISECONDS.toNanos(1000), System.nanoTime());
    analyzer.handlePauseEvent(TimeUnit.MILLISECONDS.toNanos(10000), System.nanoTime());

    PausesDetectedEvent event = analyzer.fetchEvent(Event.Severity.DEBUG, null);

    assertEquals(4, event.totalCount());
    assertEquals(10000, event.maxValue());
    assertEquals(100, event.valueAtPercentile(50.0));
    assertEquals(10000, event.valueAtPercentile(99.9));

    assertTrue(event.description().startsWith("Detected JVM pauses during the collection interval. " +
      "Total Count: 4, Max: 10000ms, " +
      "Percentiles: [50.0: 100ms, 90.0: 10000ms, 99.0: 10000ms, 99.9: 10000ms], " +
      "Collection Duration: "));
  }

}
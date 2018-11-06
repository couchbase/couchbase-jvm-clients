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
import com.couchbase.client.core.cnc.events.diagnostics.GarbageCollectionDetectedEvent;
import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

import java.lang.management.MemoryUsage;
import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

/**
 * The {@link GcAnalyzer} takes incoming {@link GarbageCollectionNotificationInfo} from the
 * JVM and analyzes it.
 *
 * <p>If the info is deemed worthy enough to be useful, an event is returned. If not, null
 * is returned.</p>
 *
 * @since 2.0.0
 */
public class GcAnalyzer
  implements Function<GarbageCollectionNotificationInfo, GarbageCollectionDetectedEvent> {

  /**
   * Holds the GC type against which this analyzer works.
   */
  private final GcType gcType;

  /**
   * Creates a new GC analyzer for the types supported.
   *
   * @param beanName the bean name of the gc.
   */
  public GcAnalyzer(final String beanName) {
    this.gcType = GcType.fromString(beanName);
    if (this.gcType == GcType.UNKNOWN) {
      throw new IllegalArgumentException("Unknown/Unsupported GC Type");
    }
  }

  @Override
  public GarbageCollectionDetectedEvent apply(final GarbageCollectionNotificationInfo info) {
    GcInfo gcInfo = info.getGcInfo();
    return new GarbageCollectionDetectedEvent(
      Event.Severity.DEBUG,
      Event.Category.SYSTEM,
      Duration.ofMillis(gcInfo.getDuration()),
      info.getGcAction(),
      info.getGcCause(),
      gcType,
      calculateUsage(gcInfo.getMemoryUsageBeforeGc()),
      calculateUsage(gcInfo.getMemoryUsageAfterGc())
    );
  }

  /**
   * Helper method to calculate the proper usage for each gc type detected.
   *
   * @param pools the pools to calculate from.
   * @return a proper value based on the gc or 0 if it couldn't be figured out.
   */
  private long calculateUsage(final Map<String, MemoryUsage> pools) {
    long usage = 0;
    try {
      switch (gcType) {
        case PS_MARK_SWEEP:
          usage += pools.get("PS Old Gen").getUsed();
          break;
        case PS_SCAVENGE:
          usage += pools.get("PS Eden Space").getUsed();
          usage += pools.get("PS Survivor Space").getUsed();
          break;
      }
    } catch (Exception ex) {
      // ignore, we just return 0 since this is best effort.
    }
    return usage;
  }

  /**
   * Specifies the generation that got collected in this event.
   */
  public enum GcGeneration {
    /**
     * The old generation has been collected.
     */
    OLD,
    /**
     * The young generation has been collected (includes eden and survivor regions).
     */
    YOUNG,
    /**
     * We couldn't figure out the generation type, sorry.
     */
    UNKNOWN
  }

  /**
   * Concurrency of a given algorithm/gc type.
   */
  public enum Concurrency {
    /**
     * The algorithm pauses the app threads while running.
     */
    STOP_THE_WORLD,
    /**
     * We couldn't figure out the concurrency type, sorry.
     */
    UNKNOWN;
  }

  /**
   * Holds the known type of garbage collectors and their representations in the
   * GC logs in textual format.
   */
  public enum GcType {

    /**
     * The young generation parallel scavenge collector.
     */
    PS_SCAVENGE("PS Scavenge", GcGeneration.YOUNG, Concurrency.STOP_THE_WORLD),
    /**
     * The old generation parallel scavenge collector.
     */
    PS_MARK_SWEEP("PS MarkSweep", GcGeneration.OLD, Concurrency.STOP_THE_WORLD),
    /**
     * We couldn't figure out the gc type, sorry!
     */
    UNKNOWN("Unknown", GcGeneration.UNKNOWN, Concurrency.UNKNOWN);

    final String identifier;
    final GcGeneration generation;
    final Concurrency concurrency;

    GcType(String identifier, GcGeneration generation, Concurrency concurrency) {
      this.identifier = identifier;
      this.generation = generation;
      this.concurrency = concurrency;
    }

    @Override
    public String toString() {
      return identifier + "(" + generation + ", " + concurrency + ")";
    }

    public static GcType fromString(final String name) {
      if (name.equals(PS_SCAVENGE.identifier)) {
        return PS_SCAVENGE;
      } else if (name.equals(PS_MARK_SWEEP.identifier)) {
        return PS_MARK_SWEEP;
      } else {
        return UNKNOWN;
      }
    }
  }

}

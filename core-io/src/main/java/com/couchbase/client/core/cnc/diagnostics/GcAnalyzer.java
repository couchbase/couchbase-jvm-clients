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

import com.couchbase.client.core.cnc.DiagnosticsMonitor;
import com.couchbase.client.core.cnc.events.diagnostics.GarbageCollectionDetectedEvent;
import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import reactor.core.publisher.Mono;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.time.Duration;
import java.util.Map;

/**
 * The {@link GcAnalyzer} takes incoming {@link GarbageCollectionNotificationInfo} from the
 * JVM and analyzes it.
 *
 * <p>If the info is deemed worthy enough to be useful, an event is returned. If not, null
 * is returned.</p>
 *
 * @since 2.0.0
 */
public class GcAnalyzer implements Analyzer, NotificationListener {

  private final DiagnosticsMonitor monitor;

  public GcAnalyzer(final DiagnosticsMonitor monitor) {
    this.monitor = monitor;
  }

  @Override
  public Mono<Void> start() {
    return Mono.defer(() -> {
      for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
        if (bean instanceof NotificationEmitter) {
          ((NotificationEmitter) bean).addNotificationListener(this, null, null);
        }
      }
      return Mono.empty();
    });
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(() -> {
      for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
        if (bean instanceof NotificationEmitter) {
          try {
            ((NotificationEmitter) bean).removeNotificationListener(this);
          } catch (ListenerNotFoundException e) {
            // ignored on purpose
          }
        }
      }
      return Mono.empty();
    });
  }

  @Override
  public void handleNotification(Notification notification, Object handback) {
    if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
      CompositeData cd = (CompositeData) notification.getUserData();
      GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(cd);
      GcInfo gcInfo = info.getGcInfo();
      GcType gcType = GcType.fromString(info.getGcName());

      if (gcType == GcType.UNKNOWN) {
        // Ignore unknown GC types
        return;
      }

      monitor.emit(new GarbageCollectionDetectedEvent(
        monitor.severity(),
        Duration.ofMillis(gcInfo.getDuration()),
        info.getGcAction(),
        info.getGcCause(),
        gcType,
        calculateUsage(gcType, gcInfo.getMemoryUsageBeforeGc()),
        calculateUsage(gcType, gcInfo.getMemoryUsageAfterGc())
      ));
    }
  }

  /**
   * Helper method to calculate the proper usage for each gc type detected.
   *
   * @param pools the pools to calculate from.
   * @return a proper value based on the gc or 0 if it couldn't be figured out.
   */
  private long calculateUsage(final GcType gcType, final Map<String, MemoryUsage> pools) {
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
        case COPY:
          usage += pools.get("Eden Space").getUsed();
          usage += pools.get("Survivor Space").getUsed();
          break;
        case MARK_SWEEP_COMPACT:
          usage += pools.get("Tenured Gen").getUsed();
          break;
        case G1_OLD:
          usage += pools.get("G1 Old Gen").getUsed();
          break;
        case G1_YOUNG:
          usage += pools.get("G1 Eden Space").getUsed();
          usage += pools.get("G1 Survivor Space").getUsed();
          break;
        case PAR_NEW:
          usage += pools.get("Par Eden Space").getUsed();
          usage += pools.get("Par Survivor Space").getUsed();
          break;
        case CONCURRENT_MARK_SWEEP:
          usage += pools.get("CMS Old Gen").getUsed();
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
     * The algorithm is concurrent and not stopping the world.
     */
    CONCURRENT,
    /**
     * We couldn't figure out the concurrency type, sorry.
     */
    UNKNOWN
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
     * The good old serial copy collector (enabled via -XX:+UseSerialGC).
     */
    COPY("Copy", GcGeneration.YOUNG, Concurrency.STOP_THE_WORLD),
    /**
     * The good old serial mark and sweep compactor (enabled via -XX:+UseSerialGC).
     */
    MARK_SWEEP_COMPACT("MarkSweepCompact", GcGeneration.OLD, Concurrency.STOP_THE_WORLD),
    /**
     * Young generation collection of the g1 collector.
     *
     * TODO: figure out the concurrency type.
     */
    G1_YOUNG("G1 Young Generation", GcGeneration.YOUNG, Concurrency.STOP_THE_WORLD),
    /**
     * Old generation collection of the g1 collector.
     *
     * TODO: figure out the concurrency type.
     */
    G1_OLD("G1 Old Generation", GcGeneration.OLD, Concurrency.STOP_THE_WORLD),
    /**
     * The ParNew collector.
     */
    PAR_NEW("ParNew", GcGeneration.YOUNG, Concurrency.STOP_THE_WORLD),
    /**
     * The concurrent mark sweep collector.
     */
    CONCURRENT_MARK_SWEEP("ConcurrentMarkSweep", GcGeneration.OLD, Concurrency.CONCURRENT),
    /**
     * We couldn't figure out the gc type, sorry!
     */
    UNKNOWN("Unknown", GcGeneration.UNKNOWN, Concurrency.UNKNOWN);

    final String identifier;
    final GcGeneration generation;
    final Concurrency concurrency;

    public String identifier() {
      return identifier;
    }

    public GcGeneration generation() {
      return generation;
    }

    public Concurrency concurrency() {
      return concurrency;
    }

    GcType(String identifier, GcGeneration generation, Concurrency concurrency) {
      this.identifier = identifier;
      this.generation = generation;
      this.concurrency = concurrency;
    }

    @Override
    public String toString() {
      return identifier + "(" + generation + ", " + concurrency + ")";
    }

    /**
     * Helper method to get the {@link GcType} for the string representation.
     *
     * @param name the name of the gc type from the JVM.
     * @return the type if found or UNKNOWN otherwise.
     */
    public static GcType fromString(final String name) {
      if (name.equals(PS_SCAVENGE.identifier)) {
        return PS_SCAVENGE;
      } else if (name.equals(PS_MARK_SWEEP.identifier)) {
        return PS_MARK_SWEEP;
      } else if (name.equals(COPY.identifier)) {
        return COPY;
      } else if (name.equals(MARK_SWEEP_COMPACT.identifier)) {
        return MARK_SWEEP_COMPACT;
      } else if (name.equals(G1_OLD.identifier)) {
        return G1_OLD;
      } else if (name.equals(G1_YOUNG.identifier)) {
        return G1_YOUNG;
      } else if (name.equals(PAR_NEW.identifier)) {
        return PAR_NEW;
      } else if (name.equals(CONCURRENT_MARK_SWEEP.identifier)) {
        return CONCURRENT_MARK_SWEEP;
      } else {
        return UNKNOWN;
      }
    }
  }

}

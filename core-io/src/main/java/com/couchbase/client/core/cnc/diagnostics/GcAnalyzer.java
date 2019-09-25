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

import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.diagnostics.GarbageCollectionsDetectedEvent;
import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import com.couchbase.client.core.deps.org.HdrHistogram.Histogram;
import com.couchbase.client.core.deps.org.HdrHistogram.Recorder;
import reactor.core.publisher.Mono;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

  private final Map<GcType, Recorder> gcHistograms;

  public GcAnalyzer() {
    gcHistograms = new ConcurrentHashMap<>();
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
  public GarbageCollectionsDetectedEvent fetchEvent(final Event.Severity severity, final Context context) {
    Map<GcType, Recorder> recorders = new HashMap<>(gcHistograms);
    Map<GcType, Histogram> histograms = new HashMap<>(recorders.size());
    for (Map.Entry<GcType, Recorder> recorder : recorders.entrySet()) {
      histograms.put(recorder.getKey(), recorder.getValue().getIntervalHistogram());
    }
    return new GarbageCollectionsDetectedEvent(severity, context, histograms);
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
  public void handleNotification(final Notification notification, final Object ignored) {
    if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
      CompositeData cd = (CompositeData) notification.getUserData();
      handleNotification(GarbageCollectionNotificationInfo.from(cd));
    }
  }

  /**
   * Helper method to handle the notification which has been already unpacked.
   *
   * <p>This method is needed in testing since it is nearly impossible to mock out the structures
   * provided by the JVM since they are under the sun namespace.</p>
   *
   * @param info the gc info.
   */
  void handleNotification(final GarbageCollectionNotificationInfo info) {
    GcInfo gcInfo = info.getGcInfo();
    GcType gcType = GcType.fromString(info.getGcName());

    synchronized (this) {
      if (!gcHistograms.containsKey(gcType)) {
        gcHistograms.put(gcType, new Recorder(4));
      }
    }

    long durationMs = gcInfo.getDuration();
    if (durationMs > 0) {
      gcHistograms.get(gcType).recordValue(durationMs);
    }
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
     * The used collector does not use a generational approach to perform GC.
     */
    NO_GENERATIONS,
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
     * The algorithm is mostly concurrent and only shortly stopping the world.
     */
    MOSTLY_CONCURRENT,
    /**
     * The algorithm is fully concurrent and not stopping the world.
     */
    FULLY_CONCURRENT,
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
     */
    G1_YOUNG("G1 Young Generation", GcGeneration.YOUNG, Concurrency.STOP_THE_WORLD),
    /**
     * Old generation collection of the g1 collector.
     */
    G1_OLD("G1 Old Generation", GcGeneration.OLD, Concurrency.MOSTLY_CONCURRENT),
    /**
     * The ParNew collector.
     */
    PAR_NEW("ParNew", GcGeneration.YOUNG, Concurrency.STOP_THE_WORLD),
    /**
     * The concurrent mark sweep collector.
     */
    CONCURRENT_MARK_SWEEP("ConcurrentMarkSweep", GcGeneration.OLD, Concurrency.MOSTLY_CONCURRENT),
    /**
     * A reported pause in shenandoah is part of a cycle and a STW phase.
     */
    SHENANDOAH_PAUSE("Shenandoah Pauses", GcGeneration.NO_GENERATIONS, Concurrency.STOP_THE_WORLD),
    /**
     * Represents a full shenandoah cycle, which includes the pauses that are reported separately.
     */
    SHENANDOAH_CYCLE("Shenandoah Cycles", GcGeneration.NO_GENERATIONS, Concurrency.MOSTLY_CONCURRENT),
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
      } else if (name.equals(SHENANDOAH_CYCLE.identifier)) {
        return SHENANDOAH_CYCLE;
      } else if (name.equals(SHENANDOAH_PAUSE.identifier)) {
        return SHENANDOAH_PAUSE;
      } else {
        return UNKNOWN;
      }
    }
  }

}

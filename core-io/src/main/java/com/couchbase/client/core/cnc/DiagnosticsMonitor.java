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

import com.couchbase.client.core.cnc.diagnostics.GcAnalyzer;
import com.sun.management.GarbageCollectionNotificationInfo;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.management.ListenerNotFoundException;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * The {@link DiagnosticsMonitor} keeps a close eye on system resources and emits events
 * if they are over a configurable threshold.
 *
 * <p>While not being 100% reliable, the monitor should provide better debugging in the
 * field and in user environments. Together with pieces like tracing the system paints
 * are more accurate picture on what's going on.</p>
 *
 * @since 2.0.0
 */
public class DiagnosticsMonitor {

  /**
   * Creates a new {@link DiagnosticsMonitor} with default settings.
   *
   * @param eventBus the event bus to dispatch the events into.
   * @return a new {@link DiagnosticsMonitor}.
   */
  public static DiagnosticsMonitor create(final EventBus eventBus) {
    return builder(eventBus).build();
  }

  /**
   * Creates a new {@link DiagnosticsMonitor} with custom settings.
   *
   * @param eventBus the event bus to dispatch the events into.
   * @return a builder to configure the monitor.
   */
  public static DiagnosticsMonitor.Builder builder(EventBus eventBus) {
    return new Builder(eventBus);
  }

  /**
   * The parent event bus.
   */
  private final EventBus eventBus;

  /**
   * All listeners this monitor opens - tracked for closing later.
   */
  private final List<NotificationListener> notificationListeners;

  /**
   * Internal method to create the new monitor from a builder config.
   *
   * @param builder the builder config.
   */
  private DiagnosticsMonitor(final Builder builder) {
    this.eventBus = builder.eventBus;
    notificationListeners = Collections.synchronizedList(new ArrayList<>());
  }

  /**
   * Starts this {@link DiagnosticsMonitor}.
   */
  public Mono<Void> start() {
    return registerGcAnalyzer();
  }

  /**
   * Stops the {@link DiagnosticsMonitor}.
   */
  public Mono<Void> stop() {
    return deregisterGcAnalyzer();
  }

  /**
   * Registers all GC analyzers through the MXBean.
   *
   * @return a mono that completes once done.
   */
  private Mono<Void> registerGcAnalyzer() {
    return Flux
      .fromIterable(ManagementFactory.getGarbageCollectorMXBeans())
      .filter(bean -> bean instanceof NotificationEmitter)
      .flatMap(bean -> {
        GcAnalyzer gcAnalyzer = new GcAnalyzer(bean.getName());
        NotificationListener listener = (notification, handback) -> {
          final String type = notification.getType();
          if (type.equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
            CompositeData cd = (CompositeData) notification.getUserData();
            Event event = gcAnalyzer.apply(GarbageCollectionNotificationInfo.from(cd));
            if (event != null) {
              eventBus.publish(event);
            }
          }
        };
        notificationListeners.add(listener);
        ((NotificationEmitter) bean).addNotificationListener(listener, null, null);
        return Mono.empty();
      })
      .then();
  }

  /**
   * Deregisters all gc analyzers that have been registered on startup.
   *
   * @return a mono that completes once done.
   */
  private Mono<Void> deregisterGcAnalyzer() {
    return Flux
      .fromIterable(ManagementFactory.getGarbageCollectorMXBeans())
      .filter(bean -> bean instanceof NotificationEmitter)
      .flatMap(bean -> {
        for (NotificationListener listener : notificationListeners) {
          try {
            ((NotificationEmitter) bean).removeNotificationListener(listener);
          } catch (ListenerNotFoundException ex) {
            // ignored, since we iterate and just remove them best effort
          }
        }
        return Mono.empty();
      })
      .then();
  }

  /**
   * Allows to configure the diagnostics monitor.
   */
  public static class Builder {

    private EventBus eventBus;

    public Builder(EventBus eventBus) {
      this.eventBus = eventBus;
    }

    public DiagnosticsMonitor build() {
      return new DiagnosticsMonitor(this);
    }

  }

}

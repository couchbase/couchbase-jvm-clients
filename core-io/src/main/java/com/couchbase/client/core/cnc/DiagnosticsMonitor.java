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

import javax.management.ListenerNotFoundException;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

  public static DiagnosticsMonitor create(EventBus eventBus) {
    return builder(eventBus).build();
  }

  public static DiagnosticsMonitor.Builder builder(EventBus eventBus) {
    return new Builder(eventBus);
  }

  private final EventBus eventBus;

  private final List<NotificationListener> notificationListeners;

  private DiagnosticsMonitor(final Builder builder) {
    this.eventBus = builder.eventBus;
    notificationListeners = Collections.synchronizedList(new ArrayList<>());
  }

  public void start() {
    registerGcAnalyzer();
  }

  public void stop() {
    deregisterGcAnalyzer();
  }

  /**
   * Registers the GC analyzer through the MXBean.
   */
  private void registerGcAnalyzer() {
    for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
      if (bean instanceof NotificationEmitter) {
        try {
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
        } catch (IllegalArgumentException ex) {
          // todo: raise an debug event which tells that the given bean is not
          // todo: support for GC tracking.
        }
      }
    }
  }

  private void deregisterGcAnalyzer() {
    for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
      if (bean instanceof NotificationEmitter) {
        for (NotificationListener listener : notificationListeners) {
          try {
            ((NotificationEmitter) bean).removeNotificationListener(listener);
          } catch (ListenerNotFoundException ex) {
            // ignored, since we iterate and just remove them best effort
          }
        }
      }
    }
  }

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

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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.diagnostics.Analyzer;
import com.couchbase.client.core.cnc.diagnostics.GcAnalyzer;
import com.couchbase.client.core.cnc.diagnostics.PauseAnalyzer;
import com.couchbase.client.core.env.DiagnosticsConfig;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
@Stability.Volatile
public class DiagnosticsMonitor {

  /**
   * Creates a new {@link DiagnosticsMonitor} with default settings.
   *
   * @param eventBus the event bus to dispatch the events into.
   * @return a new {@link DiagnosticsMonitor}.
   */
  public static DiagnosticsMonitor create(final EventBus eventBus, final DiagnosticsConfig config) {
    return new DiagnosticsMonitor(eventBus, config);
  }

  private final Duration emitInterval;

  private final Thread diagnosticsThread;
  private final AtomicBoolean diagnosticsRunning = new AtomicBoolean(true);
  private final List<Analyzer> analyzers;
  private final boolean enabled;
  private final DiagnosticsConfig config;

  /**
   * Internal method to create the new monitor from a builder config.
   *
   * @param eventBus the event bus to use.
   * @param config the config to use.
   */
  private DiagnosticsMonitor(final EventBus eventBus, final DiagnosticsConfig config) {
    this.analyzers = Collections.synchronizedList(new ArrayList<>());
    this.emitInterval = config.emitInterval();
    this.enabled = config.enabled();
    this.config = config;

    if (enabled) {
      diagnosticsThread = new Thread(() -> {
        try {
          while (diagnosticsRunning.get()) {
            Thread.sleep(emitInterval.toMillis());
            for (Analyzer analyzer : analyzers) {
              eventBus.publish(analyzer.fetchEvent(Event.Severity.INFO, context()));
            }
          }
        } catch (InterruptedException e) {
          // bail out on the interrupt.
        }
      }, "cb-diagnostics");
      diagnosticsThread.setDaemon(true);
    } else {
      diagnosticsThread = null;
    }
  }

  /**
   * Starts this {@link DiagnosticsMonitor}.
   */
  public Mono<Void> start() {
    if (!enabled) {
      return Mono.empty();
    }

    return Mono.defer(() -> {
        diagnosticsThread.start();
        return Mono.empty();
      })
      .then(Mono.defer(() -> {
          GcAnalyzer analyzer = new GcAnalyzer();
          analyzers.add(analyzer);
          return analyzer.start();
      }))
      .then(Mono.defer(() -> {
        PauseAnalyzer analyzer = new PauseAnalyzer();
        analyzers.add(analyzer);
        return analyzer.start();
      }));
  }

  /**
   * Stops the {@link DiagnosticsMonitor}.
   */
  public Mono<Void> stop() {
    if (!enabled) {
      return Mono.empty();
    }

    return Flux
      .fromIterable(analyzers)
      .flatMap(Analyzer::stop)
      .then(Mono.defer(() -> {
        diagnosticsRunning.set(false);
        diagnosticsThread.interrupt();
        return Mono.empty();
      }));
  }


  public Context context() {
    return null;
  }

  @Override
  public String toString() {
    return "DiagnosticsMonitor{" +
      "diagnosticsRunning=" + diagnosticsRunning +
      ", config=" + config +
      '}';
  }
}

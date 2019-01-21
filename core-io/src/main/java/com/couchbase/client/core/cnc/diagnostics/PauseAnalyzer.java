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
import com.couchbase.client.core.cnc.events.diagnostics.PauseDetectedEvent;
import org.LatencyUtils.PauseDetector;
import org.LatencyUtils.PauseDetectorListener;
import org.LatencyUtils.SimplePauseDetector;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Analyzes JVM pauses by utilizing the great {@link PauseDetector} from LatencyUtils.
 *
 * @since 2.0.0
 */
public class PauseAnalyzer implements PauseDetectorListener, Analyzer {

  /**
   * Not sure if this is the right default, but in early tests we've seen "noise" in the
   * low ms range that might not be effective to log. This might be tweakable in the
   * future?
   */
  private static final long LOW_THRESHOLD = TimeUnit.MILLISECONDS.toNanos(50);

  private final DiagnosticsMonitor monitor;
  private final PauseDetector detector;

  public PauseAnalyzer(final DiagnosticsMonitor monitor) {
    this.monitor = monitor;
    detector = new SimplePauseDetector();
  }

  @Override
  public Mono<Void> start() {
    return Mono.defer(() -> {
      detector.addListener(this);
      return Mono.empty();
    });
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(() -> {
      detector.removeListener(this);
      detector.shutdown();
      return Mono.empty();
    });
  }

  @Override
  public void handlePauseEvent(final long pauseLength, final long pauseEndTime) {
    if (pauseLength > LOW_THRESHOLD) {
      monitor.emit(new PauseDetectedEvent(
        monitor.severity(),
        Duration.ofNanos(pauseLength),
        monitor.context())
      );
    }
  }

}

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
import com.couchbase.client.core.cnc.events.diagnostics.PausesDetectedEvent;
import com.couchbase.client.core.deps.org.HdrHistogram.Histogram;
import com.couchbase.client.core.deps.org.HdrHistogram.Recorder;
import com.couchbase.client.core.deps.org.LatencyUtils.PauseDetector;
import com.couchbase.client.core.deps.org.LatencyUtils.PauseDetectorListener;
import com.couchbase.client.core.deps.org.LatencyUtils.SimplePauseDetector;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Analyzes JVM pauses by utilizing the great {@link PauseDetector} from LatencyUtils.
 *
 * @since 2.0.0
 */
public class PauseAnalyzer implements PauseDetectorListener, Analyzer {

  private final PauseDetector detector;

  private final Recorder recorder;

  public PauseAnalyzer() {
    detector = new SimplePauseDetector();
    recorder = new Recorder(4);
  }

  @Override
  public PausesDetectedEvent fetchEvent(Event.Severity severity, Context context) {
    Histogram histogram = recorder.getIntervalHistogram();
    return new PausesDetectedEvent(severity, context, histogram);
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
    long pauseMillis = TimeUnit.NANOSECONDS.toMillis(pauseLength);
    if (pauseMillis > 0) {
      recorder.recordValue(pauseMillis);
    }
  }

}

/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.tracing.observation;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.error.TracerException;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Wraps the Micrometer Observation, so it is suitable to be passed in into the couchbase environment and picked up
 * by the rest of the SDK as a result.
 */
public class ObservationRequestTracer implements RequestTracer {

  /**
   * Holds the ObservationRegistry.
   */
  private final ObservationRegistry observationRegistry;

  /**
   * Wraps ObservationRegistry and returns a datatype that can be passed into the requestTracer method of the
   * environment.
   *
   * @param observationRegistry the ObservationRegistry instance to wrap.
   * @return the wrapped ObservationRegistry ready to be passed in.
   */
  public static ObservationRequestTracer wrap(ObservationRegistry observationRegistry) {
    return new ObservationRequestTracer(observationRegistry);
  }

  private ObservationRequestTracer(ObservationRegistry observationRegistry) {
    this.observationRegistry = observationRegistry;
  }

  private Observation castObservation(final RequestSpan requestSpan) {
    if (requestSpan == null) {
      return null;
    }

    if (requestSpan instanceof ObservationRequestSpan) {
      return ((ObservationRequestSpan) requestSpan).observation();
    } else {
      throw new IllegalArgumentException("RequestSpan must be of type ObservationRequestSpan");
    }
  }

  /**
   * Returns the inner ObservationRegistry
   */
  public ObservationRegistry observationRegistry() {
    return observationRegistry;
  }

  @Override
  public RequestSpan requestSpan(String operationName, RequestSpan parent) {
    try {
      CouchbaseSenderContext senderContext = new CouchbaseSenderContext(operationName);
      Observation parentObservation = null;
      if (parent != null) {
        parentObservation = castObservation(parent);
        senderContext.setParentObservation(parentObservation);
      }
      parentObservation = parentObservation != null ? parentObservation : Observation.NOOP;
      // We're making the parent observation current, so that any user / framework
      // intermediate spans won't be treated as current.
      return parentObservation.scoped(() -> {
        Observation observation = Observation.createNotStarted(TracingIdentifiers.METER_OPERATIONS, () -> senderContext, observationRegistry)
          .contextualName(operationName);
        return ObservationRequestSpan.wrap(observation.start());
      });
    } catch (Exception ex) {
      throw new TracerException("Failed to create ObservationRequestSpan", ex);
    }
  }

  @Override
  public Mono<Void> start() {
    return Mono.empty(); // Tracer is not started by us
  }

  @Override
  public Mono<Void> stop(Duration timeout) {
    return Mono.empty(); // Tracer should not be stopped by us
  }

}

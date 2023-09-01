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

import java.time.Instant;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.msg.RequestContext;
import io.micrometer.observation.Observation;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Wraps a Micrometer Observation, ready to be passed in into options for each operation into the SDK as a parent.
 */
public class ObservationRequestSpan implements RequestSpan {

  /**
   * The Micrometer Observation.
   */
  private final Observation observation;

  private ObservationRequestSpan(final Observation observation) {
    this.observation = notNull(observation, "Observation");
  }

  /**
   * Wraps a Micrometer Observation so that it can be passed in to the SDK-operation options as a parent.
   *
   * @param observation the observation that should act as the parent.
   * @return the created wrapped observation.
   */
  public static ObservationRequestSpan wrap(final Observation observation) {
    return new ObservationRequestSpan(observation);
  }

  /**
   * Returns the wrapped Micrometer Observation.
   */
  public Observation observation() {
    return observation;
  }

  @Override
  public void attribute(String key, String value) {
    if (value != null) {
      observation.highCardinalityKeyValue(key, value);
    }
  }

  @Override
  public void attribute(String key, boolean value) {
    observation.highCardinalityKeyValue(key, String.valueOf(value));
  }

  @Override
  public void attribute(String key, long value) {
    observation.highCardinalityKeyValue(key, String.valueOf(value));
  }

  @Override
  public void lowCardinalityAttribute(String key, String value) {
    observation.lowCardinalityKeyValue(key, value);
  }

  @Override
  public void lowCardinalityAttribute(String key, boolean value) {
    lowCardinalityAttribute(key, String.valueOf(value));
  }

  @Override
  public void lowCardinalityAttribute(String key, long value) {
    lowCardinalityAttribute(key, String.valueOf(value));
  }

  @Override
  public void event(String name, Instant timestamp) {
    // This can lead to creation of strange metrics (name would equal metric name)
    // and we've seen users put arbitrary text (e.g. stacktraces) as span events
    // so for safety measures we are not doing any events
  }

  @Override
  public void status(StatusCode status) {
    // Not supported by Micrometer Observation
  }

  @Override
  public void recordException(Throwable err) {
    observation.error(err);
  }

  @Override
  public void end() {
    observation.stop();
  }

  @Override
  public void requestContext(final RequestContext requestContext) {
    // no need for the request context in this implementation
  }
}

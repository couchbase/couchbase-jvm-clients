/*
 * Copyright (c) 2020 Couchbase, Inc.
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
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;

/**
 * The generic interface for all metric implementations in the client.
 */
@Stability.Volatile
public interface Meter {

  /**
   * Creates a new counter.
   *
   * @param name the name of the counter.
   * @param tags the tags for the counter.
   * @return the created counter.
   */
  Counter counter(String name, Map<String, String> tags);

  /**
   * Creates a new value recorder.
   *
   * @param name the name of the value recorder.
   * @param tags the tags of the value recorder.
   * @return the created value recorder.
   */
  ValueRecorder valueRecorder(String name, Map<String, String> tags);

  /**
   * Starts the meter if it hasn't been started, might be a noop depending on the implementation.
   */
  default Mono<Void> start() {
    return Mono.empty();
  }

  /**
   * Stops the metrics if it has been started previously, might be a noop depending on the implementation.
   */
  default Mono<Void> stop(Duration timeout) {
    return Mono.empty();
  }
}

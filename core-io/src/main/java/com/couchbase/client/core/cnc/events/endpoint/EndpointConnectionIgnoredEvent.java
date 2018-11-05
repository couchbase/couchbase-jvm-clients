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

package com.couchbase.client.core.cnc.events.endpoint;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.endpoint.EndpointContext;

import java.time.Duration;
import java.util.SortedMap;

/**
 * This event is published when an endpoint is connected properly but
 * a disconnect signal came before so it is ignored.
 *
 * @since 2.0.0
 */
public class EndpointConnectionIgnoredEvent extends AbstractEvent {

  private final SortedMap<String, Duration> timings;

  public EndpointConnectionIgnoredEvent(Duration duration, EndpointContext context,
                                        final SortedMap<String, Duration> timings) {
    super(Severity.DEBUG, Category.ENDPOINT, duration, context);
    this.timings = timings;
  }

  @Override
  public String description() {
    String tm = timings == null || timings.isEmpty() ? "no timings" : "Timings: " + timings;
    return "Endpoint connected successfully, but disconnected already so ignoring. (" + tm + ")";
  }

  /**
   * Returns the timings captured as part of the endpoint connection process.
   *
   * @return the connect timings if present.
   */
  public SortedMap<String, Duration> timings() {
    return timings;
  }
}

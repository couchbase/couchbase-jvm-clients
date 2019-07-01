/*
 * Copyright (c) 2019 Couchbase, Inc.
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
import com.couchbase.client.core.endpoint.EndpointState;

import java.time.Duration;

/**
 * This event is raised when an endpoint changes its underlying state.
 */
public class EndpointStateChangedEvent extends AbstractEvent {

  private final EndpointState oldState;
  private final EndpointState newState;

  public EndpointStateChangedEvent(EndpointContext context, EndpointState oldState, EndpointState newState) {
    super(Severity.DEBUG, Category.ENDPOINT, Duration.ZERO, context);
    this.oldState = oldState;
    this.newState = newState;
  }

  public EndpointState oldState() {
    return oldState;
  }

  public EndpointState newState() {
    return newState;
  }

  @Override
  public String description() {
    return "Endpoint changed state from " + oldState + " to " + newState;
  }
}

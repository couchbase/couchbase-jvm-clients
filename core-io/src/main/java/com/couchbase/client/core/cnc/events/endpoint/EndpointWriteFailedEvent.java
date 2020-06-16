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

package com.couchbase.client.core.cnc.events.endpoint;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.endpoint.EndpointContext;

import java.time.Duration;

/**
 * This event is raised if the actual IO write and flush (into netty) failed.
 */
public class EndpointWriteFailedEvent extends AbstractEvent {

  private final Throwable cause;

  public EndpointWriteFailedEvent(final Severity severity, final EndpointContext context, final Throwable cause) {
    super(severity, Category.ENDPOINT, Duration.ZERO, context);
    this.cause = cause;
  }

  @Override
  public String description() {
    return "Writing into the channel failed unexpectedly";
  }

  @Override
  public Throwable cause() {
    return cause;
  }
}

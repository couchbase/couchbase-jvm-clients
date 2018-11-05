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

/**
 * This event is raised if an endpoint disconnect attempt failed for some reason.
 *
 * @since 2.0.0
 */
public class EndpointDisconnectionFailedEvent extends AbstractEvent {

  private final Throwable cause;

  /**
   * Creates a new endpoint disconnection failed event.
   *
   * @param duration the duration of the event.
   * @param context the endpoint context.
   * @param cause the cause of the failure.
   */
  public EndpointDisconnectionFailedEvent(final Duration duration, final EndpointContext context,
                                          final Throwable cause) {
    super(Severity.WARN, Category.ENDPOINT, duration, context);
    this.cause = cause;
  }

  /**
   * Returns the cause of the failure.
   */
  public Throwable cause() {
    return cause;
  }

  @Override
  public String description() {
    String msg = cause == null ? "" : " because of " + cause.getClass().getSimpleName();
    return "Endpoint disconnect failed" + msg;
  }
}

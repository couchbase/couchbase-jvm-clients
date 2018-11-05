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
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;

/**
 * During connecting to an endpoint, an exception/error was raised which was not expected. This
 * should be reported as a bug.
 *
 * @since 2.0.0
 */
public class UnexpectedEndpointConnectionFailedEvent extends AbstractEvent {

  private final Throwable cause;

  public UnexpectedEndpointConnectionFailedEvent(final Duration duration, final Context context,
                                                 final Throwable cause) {
    super(Severity.WARN, Category.ENDPOINT, duration, context);
    this.cause = cause;
  }

  /**
   * The cause of the unexpected connecting error.
   *
   * @return the cause.
   */
  public Throwable cause() {
    return cause;
  }

  @Override
  public String description() {
    return "An unexpected error happened during a connection event. "
      + "This is a bug, please report. "
      + cause;
  }
}

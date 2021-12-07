/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.events.core;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;

/**
 * Raised when the watchdog itself runs into an error that is logged.
 */
public class WatchdogRunFailedEvent extends AbstractEvent {

  private final Throwable cause;

  public WatchdogRunFailedEvent(final Context context, final Throwable cause) {
    super(Severity.WARN, Category.CORE, Duration.ZERO, context);
    this.cause = cause;
  }

  @Override
  public String description() {
    return "The core InvalidStateWatchdog failed with an error.";
  }

  @Override
  public Throwable cause() {
    return cause;
  }

}

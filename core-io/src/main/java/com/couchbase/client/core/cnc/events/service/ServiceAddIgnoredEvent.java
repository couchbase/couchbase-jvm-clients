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

package com.couchbase.client.core.cnc.events.service;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;

import java.time.Duration;

public class ServiceAddIgnoredEvent extends AbstractEvent {

  private final Reason reason;

  public ServiceAddIgnoredEvent(final Severity severity, final Reason reason,
                                final Context context) {
    super(severity, Category.SERVICE, Duration.ZERO, context);
    this.reason = reason;
  }

  public Reason reason() {
    return reason;
  }

  @Override
  public String description() {
    return "Service not added to Node. Reason: " + reason;
  }

  /**
   * Enumerates the reasons why a add service event could be ignored by the system.
   */
  public enum Reason {
    /**
     * Add service ignored because it has been already added.
     */
    ALREADY_ADDED,
    /**
     * Add service ignored because the node is disconnected already.
     */
    DISCONNECTED
  }
}

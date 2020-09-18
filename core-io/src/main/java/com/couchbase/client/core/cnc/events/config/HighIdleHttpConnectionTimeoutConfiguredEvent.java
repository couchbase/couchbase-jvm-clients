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

package com.couchbase.client.core.cnc.events.config;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.service.AbstractPooledEndpointServiceConfig;

import java.time.Duration;

/**
 * This event is raised if the user configures a idleHttpConnectionTimeout over 4.5 seconds, which will have some
 * negative side effects on most services.
 */
public class HighIdleHttpConnectionTimeoutConfiguredEvent extends AbstractEvent {

  public HighIdleHttpConnectionTimeoutConfiguredEvent() {
    super(Severity.INFO, Category.CORE, Duration.ZERO, null);
  }

  @Override
  public String description() {
    return "A idleHttpConnectionTimeout over "+ AbstractPooledEndpointServiceConfig.DEFAULT_IDLE_TIME.toMillis() + "ms has been configured - most services will " +
      "terminate idle connections after this period and you will see reconnect warnings in the log.";
  }

}

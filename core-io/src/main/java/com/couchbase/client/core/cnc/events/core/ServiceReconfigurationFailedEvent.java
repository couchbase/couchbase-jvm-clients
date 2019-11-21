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

package com.couchbase.client.core.cnc.events.core;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

public class ServiceReconfigurationFailedEvent extends AbstractEvent {

  private final String hostname;
  private final ServiceType serviceType;
  private final Throwable reason;

  public ServiceReconfigurationFailedEvent(Context context, String hostname,
                                           ServiceType serviceType, Throwable reason) {
    super(Severity.WARN, Category.CORE, Duration.ZERO, context);
    this.reason = reason;
    this.hostname = hostname;
    this.serviceType = serviceType;
  }

  @Override
  public Throwable cause() {
    return reason;
  }

  public String hostname() {
    return hostname;
  }

  public ServiceType serviceType() {
    return serviceType;
  }

  @Override
  public String description() {
    return "Service " + serviceType + " on " + redactSystem(hostname) + " failed to reconfigure: "
      + reason.getMessage();
  }
}

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

package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;

/**
 * If this event is raised, it indicates a serious bug in the system.
 *
 * <p>Basically what happened is that the endpoint dispatched a request of a type it has no
 * idea about into the IO layer. This needs to be reported as an issue to the maintainers.</p>
 *
 * @since 2.0.0
 */
public class InvalidRequestDetectedEvent extends AbstractEvent {

  private final ServiceType serviceType;
  private final Object message;

  public InvalidRequestDetectedEvent(IoContext context, ServiceType serviceType, Object message) {
    super(Severity.ERROR, Category.IO, Duration.ZERO, context);
    this.message = message;
    this.serviceType = serviceType;
  }

  public Object message() {
    return message;
  }

  public ServiceType serviceType() {
    return serviceType;
  }

  @Override
  public String description() {
    return "On IO Handler for Service " + serviceType + ", detected invalid request " + message;
  }
}

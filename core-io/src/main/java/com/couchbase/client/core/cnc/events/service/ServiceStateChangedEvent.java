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

package com.couchbase.client.core.cnc.events.service;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceContext;
import com.couchbase.client.core.service.ServiceState;

import java.time.Duration;

/**
 * This event is raised every time a {@link Service} changes its state.
 */
public class ServiceStateChangedEvent extends AbstractEvent {

  private final ServiceState oldState;
  private final ServiceState newState;

  public ServiceStateChangedEvent(final ServiceContext context, final ServiceState oldState,
                                  final ServiceState newState) {
    super(Severity.DEBUG, Category.SERVICE, Duration.ZERO, context);
    this.oldState = oldState;
    this.newState = newState;
  }

  public ServiceState oldState() {
    return oldState;
  }

  public ServiceState newState() {
    return newState;
  }

  @Override
  public String description() {
    return "Service changed state from " + oldState + " to " + newState;
  }

}

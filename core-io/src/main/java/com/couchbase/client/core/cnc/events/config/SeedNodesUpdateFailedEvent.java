/*
 * Copyright (c) 2022 Couchbase, Inc.
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
import com.couchbase.client.core.cnc.Context;
import reactor.core.publisher.Sinks;

import java.time.Duration;

/**
 * This event is raised if a seed nodes update cannot be pushed to downstream subscribers.
 */
public class SeedNodesUpdateFailedEvent extends AbstractEvent {

  private final Sinks.EmitResult reason;

  public SeedNodesUpdateFailedEvent(Context context, Sinks.EmitResult reason) {
    super(Severity.WARN, Category.CONFIG, Duration.ZERO, context);
    this.reason = reason;
  }

  @Override
  public String description() {
    return "Seed node update failed because of: " + reason;
  }

}

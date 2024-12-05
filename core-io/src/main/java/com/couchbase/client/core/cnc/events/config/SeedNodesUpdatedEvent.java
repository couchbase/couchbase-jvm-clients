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

package com.couchbase.client.core.cnc.events.config;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.env.SeedNode;

import java.time.Duration;
import java.util.Set;

/**
 * This event is emitted when seed nodes are updated in the configuration provider.
 */
public class SeedNodesUpdatedEvent extends AbstractEvent {

  private final Set<SeedNode> oldSeedNodes;
  private final Set<SeedNode> newSeedNodes;

  public SeedNodesUpdatedEvent(final Context context, final Set<SeedNode> oldSeedNodes, final Set<SeedNode> newSeedNodes) {
    super(Severity.DEBUG, Category.CONFIG, Duration.ZERO, context);
    this.oldSeedNodes = oldSeedNodes;
    this.newSeedNodes = newSeedNodes;
  }

  @Override
  public String description() {
    return oldSeedNodes.equals(newSeedNodes)
      ? "Seed nodes unchanged: " + newSeedNodes
      : "Seed nodes updated from " + oldSeedNodes + " to " + newSeedNodes;
  }

}

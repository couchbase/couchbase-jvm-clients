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

package com.couchbase.client.core.cnc.events.node;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.node.NodeContext;

import java.time.Duration;

/**
 * This event is generated when a node is correctly connected.
 *
 * @since 2.0.0
 */
public class NodeConnectedEvent extends AbstractEvent {

  /**
   * Creates a new node connected event.
   *
   * @param duration the duration of the event.
   * @param context the context.
   */
  public NodeConnectedEvent(final Duration duration, final NodeContext context) {
    super(Severity.INFO, Category.NODE, duration, context);
  }

  @Override
  public String description() {
    return "Node connected";
  }
}

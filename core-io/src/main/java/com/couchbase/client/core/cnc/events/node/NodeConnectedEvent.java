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
import com.couchbase.client.core.node.NodeContext;

import java.time.Duration;

/**
 * Sent when a node is created, prior to connecting to services on the node.
 *
 * @since 2.0.0
 * @deprecated This event is sent when a node is created, which happens before network
 * connections are established. To avoid confusion, this event is deprecated in favor of
 * the more appropriately named {@link NodeCreatedEvent}.
 */
@Deprecated
public class NodeConnectedEvent extends AbstractEvent {

  /**
   * Creates a new node connected event.
   *
   * @param duration the duration of the event.
   * @param context the context.
   */
  public NodeConnectedEvent(final Duration duration, final NodeContext context) {
    super(Severity.VERBOSE, Category.NODE, duration, context);
  }

  @Override
  public String description() {
    return "This event type is deprecated, because it does not actually signify a network connection; please listen for NodeCreatedEvent instead";
  }
}

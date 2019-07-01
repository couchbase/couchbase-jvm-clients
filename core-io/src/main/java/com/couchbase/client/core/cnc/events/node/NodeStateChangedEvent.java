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

package com.couchbase.client.core.cnc.events.node;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.node.NodeContext;
import com.couchbase.client.core.node.NodeState;

import java.time.Duration;

public class NodeStateChangedEvent extends AbstractEvent {

  private final NodeState oldState;
  private final NodeState newState;

  public NodeStateChangedEvent(NodeContext context, NodeState oldState, NodeState newState) {
    super(Severity.DEBUG, Category.NODE, Duration.ZERO, context);
    this.oldState = oldState;
    this.newState = newState;
  }

  public NodeState oldState() {
    return oldState;
  }

  public NodeState newState() {
    return newState;
  }

  @Override
  public String description() {
    return "Node changed state from " + oldState + " to " + newState;
  }
}

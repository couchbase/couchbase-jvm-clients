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

package com.couchbase.client.core.node;

/**
 * Holds all the different states a {@link Node} can be in.
 */
public enum NodeState {
  /**
   * This {@link Node} is idle (no services attached at all or all of them idle)
   */
  IDLE,
  /**
   * This {@link Node} is disconnected (has services and all are disconnected)
   */
  DISCONNECTED,
  /**
   * This {@link Node} has all services connecting.
   */
  CONNECTING,
  /**
   * This {@link Node} has all services connected.
   */
  CONNECTED,
  /**
   * This {@link Node} has all services disconnecting.
   */
  DISCONNECTING,
  /**
   * This {@link Node} has at least one service connected.
   */
  DEGRADED

}

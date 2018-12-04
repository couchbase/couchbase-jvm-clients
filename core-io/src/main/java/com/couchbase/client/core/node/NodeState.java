package com.couchbase.client.core.node;


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
  DEGRADED,
}

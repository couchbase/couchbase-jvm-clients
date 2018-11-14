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

package com.couchbase.client.core.service;

/**
 * Holds all states a {@link Service} can be in.
 *
 * @since 2.0.0
 */
public enum ServiceState {
  /**
   * This {@link Service} is idle (no endpoints attached at all)
   */
  IDLE,
  /**
   * This {@link Service} is disconnected (has endpoints and all are disconnected)
   */
  DISCONNECTED,
  /**
   * This {@link Service} has all endpoints connecting.
   */
  CONNECTING,
  /**
   * This {@link Service} has all endpoints connected.
   */
  CONNECTED,
  /**
   * This {@link Service} has all endpoints disconnecting.
   */
  DISCONNECTING,
  /**
   * This {@link Service} has at least one endpoint connected.
   */
  DEGRADED,
}

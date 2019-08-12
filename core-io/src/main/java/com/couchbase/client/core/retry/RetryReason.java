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

package com.couchbase.client.core.retry;

import com.couchbase.client.core.annotation.Stability;

/**
 * Provides more insight into why an operation has been retried.
 */
@Stability.Volatile
public enum RetryReason {
  /**
   * The reason why it has been retried is unknown.
   */
  UNKNOWN,
  /**
   * Retried because at the point in time there was no endpoint available to dispatch to.
   */
  NO_ENDPOINT_AVAILABLE,
  /**
   * Retried because at this point in time there is no service available to dispatch to.
   */
  NO_SERVICE_AVAILBLE,
  /**
   * Retried because at this point in time there is no node available to dispatch to.
   */
  NO_NODE_AVAILABLE,
  /**
   * A KV "not my vbucket" response has been received.
   */
  KV_NOT_MY_VBUCKET,
  /**
   * The collection identifier for the KV service has been outdated.
   */
  KV_COLLECTION_OUTDATED,
  /**
   * The request has been dispatched into a non-pipelined handler and a request is currently
   * in-flight so it cannot be dispatched right now onto the same socket.
   */
  NOT_PIPELINED_REQUEST_IN_FLIGHT,
  /**
   * The endpoint is connected, but for some reason cannot be written to at the moment.
   */
  ENDPOINT_NOT_WRITABLE,
}

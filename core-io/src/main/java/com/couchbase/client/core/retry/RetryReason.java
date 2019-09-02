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
  UNKNOWN(false, false),
  /**
   * Retried because at the point in time there was no endpoint available to dispatch to.
   */
  ENDPOINT_NOT_AVAILABLE(true, false),
  /**
   * Retried because no endpoint available, but a new one being opened in parallel.
   */
  ENDPOINT_TEMPORARILY_NOT_AVAILABLE(true, true),
  /**
   * Retried because at this point in time there is no service available to dispatch to.
   */
  SERVICE_NOT_AVAILABLE(true, false),
  /**
   * Retried because at this point in time there is no node available to dispatch to.
   */
  NODE_NOT_AVAILABLE(true, false),
  /**
   * A KV "not my vbucket" response has been received.
   */
  KV_NOT_MY_VBUCKET(true, true),
  /**
   * The collection identifier for the KV service has been outdated.
   */
  KV_COLLECTION_OUTDATED(true, true),
  /**
   * The KV error map indicated a retry action on an unknown response code.
   */
  KV_ERROR_MAP_INDICATED(true, false),
  /**
   * Server response indicates a locked document.
   */
  KV_LOCKED(true, false),
  /**
   * Server response indicates a temporary failure.
   */
  KV_TEMPORARY_FAILURE(true, false),
  /**
   * Server response indicates a sync write in progress.
   */
  KV_SYNC_WRITE_IN_PROGRESS(true, false),
  /**
   * Server response a sync write re-commit in progress.
   */
  KV_SYNC_WRITE_RE_COMMIT_IN_PROGRESS(true, false),
  /**
   * The request has been dispatched into a non-pipelined handler and a request is currently
   * in-flight so it cannot be dispatched right now onto the same socket.
   */
  NOT_PIPELINED_REQUEST_IN_FLIGHT(true, true),
  /**
   * The endpoint is connected, but for some reason cannot be written to at the moment.
   */
  ENDPOINT_NOT_WRITABLE(true, false),
  /**
   * The underlying channel on the endpoint closed while this operation was still in-flight and we
   * do not have a response yet.
   */
  CHANNEL_CLOSED_WHILE_IN_FLIGHT(false, false);

  private final boolean allowsNonIdempotentRetry;
  private final boolean alwaysRetry;

  RetryReason(boolean allowsNonIdempotentRetry, boolean alwaysRetry) {
    this.allowsNonIdempotentRetry = allowsNonIdempotentRetry;
    this.alwaysRetry = alwaysRetry;
  }

  boolean allowsNonIdempotentRetry() {
    return allowsNonIdempotentRetry;
  }

  boolean alwaysRetry() {
    return alwaysRetry;
  }

}

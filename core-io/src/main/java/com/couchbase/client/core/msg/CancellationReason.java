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

package com.couchbase.client.core.msg;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonValue;
import com.couchbase.client.core.retry.RetryReason;

import java.util.Objects;

/**
 * Describes the reason why a {@link Request} has been cancelled.
 *
 * @since 2.0.0
 */
public class CancellationReason {

  /**
   * The downstream consumer stopped listening for a result and therefore any further
   * processing is a waste of resources.
   */
  public static final CancellationReason STOPPED_LISTENING =
    new CancellationReason("STOPPED_LISTENING", null);

  /**
   * The request ran into a timeout and is therefore cancelled before it got a chance
   * to complete.
   */
  public static final CancellationReason TIMEOUT =
    new CancellationReason("TIMEOUT", null);

  /**
   * The user or some other code proactively cancelled the request by cancelling
   * it through its attached context.
   */
  public static final CancellationReason CANCELLED_VIA_CONTEXT =
    new CancellationReason("CANCELLED_VIA_CONTEXT", null);

  /**
   * The SDK has been shut down already when this request is dispatched.
   */
  public static final CancellationReason SHUTDOWN =
    new CancellationReason("SHUTDOWN", null);

  /**
   * For a different reason. Make sure to emit an event so that debugging provides
   * further context.
   */
  public static final CancellationReason OTHER =
    new CancellationReason("OTHER", null);

  /**
   * If too many outstanding requests are waiting to be completed. This is the SDK backpressure signal.
   */
  public static final CancellationReason TOO_MANY_REQUESTS_IN_RETRY =
    new CancellationReason("TOO_MANY_REQUESTS_IN_RETRY", null);

  /**
   * When a {@link TargetedRequest} is dispatched but the list of nodes does not contain the target at all,
   * there is good chance that this request will not be able to make progress anymore so it will be cancelled.
   * <p>
   * Request creators are advised to grab a fresh config, a new target, and re-dispatch the operation to give
   * it a chance to make progress eventually.
   */
  public static final CancellationReason TARGET_NODE_REMOVED =
    new CancellationReason("TARGET_NODE_REMOVED", null);

  /**
   * The server reported that it cancelled the request.
   */
  public static final CancellationReason SERVER_CANCELLED =
    new CancellationReason("SERVER_CANCELLED", null);

  private final String identifier;
  private final Object innerReason;

  private CancellationReason(final String identifier, final Object innerReason) {
    this.innerReason = innerReason;
    this.identifier = identifier;
  }

  /**
   * This cancellation reason indicates that no more retries were allowed based on the retry strategy.
   *
   * @param retryReason the retry reason why it got sent into retry.
   * @return the cancellation reason instance.
   */
  public static CancellationReason noMoreRetries(final RetryReason retryReason) {
    return new CancellationReason("NO_MORE_RETRIES", retryReason);
  }

  /**
   * If applicable, returns an inner reason for the cancellation for additional context.
   */
  public Object innerReason() {
    return innerReason;
  }

  /**
   * Returns the identifier for this reason.
   */
  public String identifier() {
    return identifier;
  }

  @Override
  @JsonValue
  public String toString() {
    String inner = innerReason != null ? (" (" + innerReason.toString()+ ")") : "";
    return identifier + inner;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CancellationReason that = (CancellationReason) o;
    return Objects.equals(identifier, that.identifier) &&
      Objects.equals(innerReason, that.innerReason);
  }

  @Override
  public int hashCode() {
    return Objects.hash(identifier, innerReason);
  }
}

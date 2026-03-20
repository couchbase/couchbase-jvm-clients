/*
 * Copyright (c) 2023 Couchbase, Inc.
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
import org.jspecify.annotations.Nullable;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Determines what to do with a request.
 * <p>
 * Exactly one of these conditions must be true:
 * <p>
 * - retryDuration is non-null, indicating the operation should be retried
 * - exception is non-null, indicating the operation should be failed to the user
 * - returnNull is true, indicating the operation should succeed and return null to the user
 * - if all the above are false, a non-null success (operation-dependent) is returned to the user
 */
@Stability.Internal
public class ProtostellarRequestBehaviour {
  /**
   * If not-null, the request will be retried after this duration.
   */
  private final @Nullable Duration retryDuration;

  /**
   * If not-null, this exception will be raised.
   */
  private final @Nullable RuntimeException exception;

  /**
   * If true, a successful null result is returned to the user.
   */
  private final boolean returnNull;

  private ProtostellarRequestBehaviour(@Nullable Duration retryDuration, @Nullable RuntimeException exception, boolean returnNull) {
    if (returnNull && (retryDuration != null || exception != null)) {
      throw new IllegalArgumentException("Internal bug: cannot combine returnNull with errors");
    }
    this.retryDuration = retryDuration;
    this.exception = exception;
    this.returnNull = returnNull;
  }

  public static ProtostellarRequestBehaviour retry(Duration retryDuration) {
    requireNonNull(retryDuration);
    return new ProtostellarRequestBehaviour(retryDuration, null, false);
  }

  public static ProtostellarRequestBehaviour fail(RuntimeException err) {
    requireNonNull(err);
    return new ProtostellarRequestBehaviour(null, err, false);
  }

  public static ProtostellarRequestBehaviour success() {
    return new ProtostellarRequestBehaviour(null, null, false);
  }

  public static ProtostellarRequestBehaviour successReturningNull() {
    return new ProtostellarRequestBehaviour(null, null, true);
  }

  public @Nullable Duration retryDuration() {
    return retryDuration;
  }

  public @Nullable RuntimeException exception() {
    return exception;
  }

  public boolean returnNull() {
    return returnNull;
  }

  @Override
  public String toString() {
    return "ProtostellarRequestBehaviour{" +
            "retryDuration=" + retryDuration +
            ", exception=" + exception +
            ", returnNull=" + returnNull +
            '}';
  }
}

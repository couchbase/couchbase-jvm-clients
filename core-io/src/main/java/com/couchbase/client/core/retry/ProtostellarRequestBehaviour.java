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
 * This is an Either - if retryDuration is null, RuntimeException must not be.
 * <p>
 * Both can be null, in which case the operation succeeded (used for LookupIn).
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

  private ProtostellarRequestBehaviour(@Nullable Duration retryDuration, @Nullable RuntimeException exception) {
    this.retryDuration = retryDuration;
    this.exception = exception;
  }

  public static ProtostellarRequestBehaviour retry(Duration retryDuration) {
    requireNonNull(retryDuration);
    return new ProtostellarRequestBehaviour(retryDuration, null);
  }

  public static ProtostellarRequestBehaviour fail(RuntimeException err) {
    requireNonNull(err);
    return new ProtostellarRequestBehaviour(null, err);
  }

  public static ProtostellarRequestBehaviour success() {
    return new ProtostellarRequestBehaviour(null, null);
  }

  public @Nullable Duration retryDuration() {
    return retryDuration;
  }

  public @Nullable RuntimeException exception() {
    return exception;
  }
}

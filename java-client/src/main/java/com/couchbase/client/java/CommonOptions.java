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

package com.couchbase.client.java;

import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;

/**
 * Common options that are used by most operations.
 *
 * @since 2.0.0
 */
public abstract class CommonOptions<SELF extends CommonOptions<SELF>> {

  /**
   * The timeout for the operation, if set.
   */
  private Duration timeout;

  /**
   * The custom retry strategy, if set.
   */
  private RetryStrategy retryStrategy;

  @SuppressWarnings({ "unchecked" })
  protected SELF self() {
    return (SELF) this;
  }

  public SELF timeout(final Duration timeout) {
    this.timeout = timeout;
    return self();
  }

  public Duration timeout() {
    return timeout;
  }

  public SELF retryStrategy(final RetryStrategy retryStrategy) {
    this.retryStrategy = retryStrategy;
    return self();
  }

  public RetryStrategy retryStrategy() {
    return retryStrategy;
  }

}

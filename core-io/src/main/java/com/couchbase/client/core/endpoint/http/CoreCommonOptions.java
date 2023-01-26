/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.endpoint.http;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.retry.RetryStrategy;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@Stability.Internal
public interface CoreCommonOptions {
  CoreCommonOptions DEFAULT = new CoreCommonOptions() {
    @Override
    public Optional<Duration> timeout() {
      return Optional.empty();
    }

    @Override
    public Optional<RetryStrategy> retryStrategy() {
      return Optional.empty();
    }

    @Override
    public Optional<RequestSpan> parentSpan() {
      return Optional.empty();
    }
  };

  Optional<Duration> timeout();

  Optional<RetryStrategy> retryStrategy();

  Optional<RequestSpan> parentSpan();

  @Nullable
  default Map<String, Object> clientContext() { return null; }

  static CoreCommonOptions of(Duration timeout, RetryStrategy retryStrategy, RequestSpan parentSpan) {
    if (timeout == null && retryStrategy == null && parentSpan == null) {
      return DEFAULT;
    }

    return new CoreCommonOptions() {
      @Override
      public Optional<Duration> timeout() {
        return Optional.ofNullable(timeout);
      }

      @Override
      public Optional<RetryStrategy> retryStrategy() {
        return Optional.ofNullable(retryStrategy);
      }

      @Override
      public Optional<RequestSpan> parentSpan() {
        return Optional.ofNullable(parentSpan);
      }
    };
  }

  default CoreCommonOptions withParentSpan(RequestSpan span) {
    return CoreCommonOptions.of(timeout().orElse(null), retryStrategy().orElse(null), span);
  }
}

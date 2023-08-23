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

package com.couchbase.client.core.service.strategy;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.service.EndpointSelectionStrategy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinSelectionStrategy implements EndpointSelectionStrategy {

  private final AtomicInteger skip = new AtomicInteger(0);

  @Override
  public <R extends Request<? extends Response>> Endpoint select(final R request,
                                                                 final List<Endpoint> endpoints) {
    // `endpoints` is mutable and might be modified concurrently, so check again for empty list
    // to prevent the upcoming % operations from throwing ArithmeticException.
    int endpointsSize = endpoints.size();
    if (endpointsSize == 0) {
      return null;
    }

    int startIndex = forcePositive(skip.incrementAndGet()) % endpointsSize;

    // Search for a connected endpoint, starting at startOffset, and wrapping around if necessary.
    for (int i = 0; i < endpointsSize; i++) {
      try {
        Endpoint endpoint = endpoints.get((startIndex + i) % endpointsSize);
        if (endpoint.state() == EndpointState.CONNECTED && endpoint.freeToWrite()) {
          return endpoint;
        }
      } catch (IndexOutOfBoundsException ignore) {
        // Endpoint list was modified concurrently, and there's no longer
        // an element at this index. Continue, because later iterations might
        // wrap around to the start of the list.
      }
    }

    //lastly, really no eligible endpoint was found, return null
    return null;
  }

  /**
   * Force a value to the skip counter, mainly for testing purposes.
   *
   * @param newValue the new skip value to apply.
   */
  void setSkip(int newValue) {
    skip.set(newValue < 0 ? 0 : newValue);
  }

  /**
   * Returns the current skip value, useful for testing.
   */
  int currentSkip() {
    return forcePositive(skip.get());
  }

  private static int forcePositive(int i) {
    return i & 0x7fffffff; // clear the sign bit
  }
}

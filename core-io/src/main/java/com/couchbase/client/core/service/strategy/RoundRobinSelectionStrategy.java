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
    int endpointSize = endpoints.size();
    //increments skip and prevents it to overflow to a negative value
    skip.set(Math.max(0, skip.get()+1));
    int offset = skip.get() % endpointSize;

    //attempt to find a CONNECTED endpoint at the offset, or try following ones
    for (int i = offset; i < endpointSize; i++) {
      Endpoint endpoint = endpoints.get(i);
      if (endpoint.state() == EndpointState.CONNECTED && endpoint.freeToWrite()) {
        return endpoint;
      }
    }

    //arriving here means the offset endpoint wasn't CONNECTED and none of the endpoints after it were.
    //wrap around and try from the beginning of the array
    for (int i = 0; i < offset; i++) {
      Endpoint endpoint = endpoints.get(i);
      if (endpoint.state() == EndpointState.CONNECTED && endpoint.freeToWrite()) {
        return endpoint;
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
    return skip.get();
  }

}

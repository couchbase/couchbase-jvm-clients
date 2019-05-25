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
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.service.EndpointSelectionStrategy;

import java.util.List;

public class PartitionSelectionStrategy implements EndpointSelectionStrategy {

  @Override
  public <R extends Request<? extends Response>> Endpoint select(final R request, final List<Endpoint> endpoints) {
    int size = endpoints.size();
    if (size == 0) {
      return null;
    }

    short partition = ((KeyValueRequest<?>) request).partition();
    Endpoint endpoint = size == 1 ? endpoints.get(0) : endpoints.get(partition % size);
    if (endpoint != null && endpoint.state() == EndpointState.CONNECTED && endpoint.free()) {
      return endpoint;
    }

    return null;
  }

}

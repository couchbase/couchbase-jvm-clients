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

package com.couchbase.client.core.service;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;

import java.util.List;

public interface EndpointSelectionStrategy {

  /**
   * Selects an {@link Endpoint} for the given {@link Request}.
   *
   * <p>If null is returned, it means that no endpoint could be selected and it is up to the
   * calling party to decide what to do next.</p>
   *
   * @param request the input request.
   * @param endpoints all the available endpoints.
   * @return the selected endpoint.
   */
  <R extends Request<? extends Response>> Endpoint select(R request, List<Endpoint> endpoints);

}

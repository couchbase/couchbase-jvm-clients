/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.java.manager.raw;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Cluster;

@Stability.Uncommitted
public class RawManagerRequest {

  private final ServiceType serviceType;
  private final String uri;
  private final HttpMethod method;

  private RawManagerRequest(final ServiceType serviceType, final HttpMethod method, final String uri) {
    this.serviceType = serviceType;
    this.method = method;
    this.uri = uri;
  }

  /**
   * Performs a HTP GET request against the given service type and URI.
   *
   * @param serviceType the service type to query.
   * @param uri the URI to query.
   * @return a request that should be passed into {@link RawManager#call(Cluster, RawManagerRequest)}.
   */
  public static RawManagerRequest get(final ServiceType serviceType, final String uri) {
    return new RawManagerRequest(serviceType, HttpMethod.GET, uri);
  }

  ServiceType serviceType() {
    return serviceType;
  }

  String uri() {
    return uri;
  }

  HttpMethod method() {
    return method;
  }
}

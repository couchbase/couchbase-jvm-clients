/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.http;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.service.ServiceType;

import static java.util.Objects.requireNonNull;

/**
 * Specifies which Couchbase service should receive the request.
 * <p>
 * Create an instance using one of the static factory methods.
 *
 * @see #analytics()
 * @see #backup()
 * @see #eventing()
 * @see #manager()
 * @see #query()
 * @see #search()
 */
public class HttpTarget {
  final RequestTarget coreTarget;

  private HttpTarget(ServiceType service) {
    this(service, null, null);
  }

  /**
   * @param node (nullable)
   * @param bucket (nullable)
   */
  private HttpTarget(ServiceType service, NodeIdentifier node, String bucket) {
    this.coreTarget = new RequestTarget(service, node, bucket);
  }

  /**
   * Returns a copy of this target with the given node identifier.
   *
   * @param node (nullable) null means let the SDK pick a node to receive the request.
   */
  @Stability.Internal
  public HttpTarget withNode(NodeIdentifier node) {
    return new HttpTarget(coreTarget.serviceType(), node, coreTarget.bucketName());
  }

  @Override
  public String toString() {
    return "HttpTarget{" + coreTarget + "}";
  }

  public static HttpTarget analytics() {
    return new HttpTarget(ServiceType.ANALYTICS);
  }

  public static HttpTarget backup() {
    return new HttpTarget(ServiceType.BACKUP);
  }

  public static HttpTarget eventing() {
    return new HttpTarget(ServiceType.EVENTING);
  }

  public static HttpTarget manager() {
    return new HttpTarget(ServiceType.MANAGER);
  }

  public static HttpTarget query() {
    return new HttpTarget(ServiceType.QUERY);
  }

  public static HttpTarget search() {
    return new HttpTarget(ServiceType.SEARCH);
  }
}

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

package com.couchbase.client.core.msg;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.service.ServiceType;

import java.util.Objects;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class RequestTarget {
  private final ServiceType serviceType;
  private final NodeIdentifier nodeIdentifier;
  private final String bucketName;

  public static RequestTarget views(String bucket) {
    return new RequestTarget(ServiceType.VIEWS, null, bucket);
  }

  public static RequestTarget manager() {
    return new RequestTarget(ServiceType.MANAGER, null, null);
  }

  public static RequestTarget query() {
    return new RequestTarget(ServiceType.QUERY, null, null);
  }

  public static RequestTarget analytics() {
    return new RequestTarget(ServiceType.ANALYTICS, null, null);
  }

  public static RequestTarget search() {
    return new RequestTarget(ServiceType.SEARCH, null, null);
  }

  public static RequestTarget eventing() {
    return new RequestTarget(ServiceType.EVENTING, null, null);
  }

  /**
   * @param nodeIdentifier (nullable)
   * @param bucketName (nullable)
   */
  public RequestTarget(ServiceType serviceType, NodeIdentifier nodeIdentifier, String bucketName) {
    this.serviceType = requireNonNull(serviceType);
    this.nodeIdentifier = nodeIdentifier;
    this.bucketName = bucketName;
  }

  /**
   * @param nodeIdentifier (nullable)
   */
  public RequestTarget withNodeIdentifier(NodeIdentifier nodeIdentifier) {
    return new RequestTarget(serviceType, nodeIdentifier, bucketName);
  }

  public ServiceType serviceType() {
    return serviceType;
  }

  /**
   * @return (nullable)
   */
  public String bucketName() {
    return bucketName;
  }

  /**
   * @return (nullable)
   */
  public NodeIdentifier nodeIdentifier() {
    return nodeIdentifier;
  }

  @Override
  public String toString() {
    return "RequestTarget{" +
        "serviceType=" + serviceType +
        ", nodeIdentifier=" + redactSystem(nodeIdentifier) +
        ", bucketName='" + redactMeta(bucketName) + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RequestTarget that = (RequestTarget) o;
    return serviceType == that.serviceType && Objects.equals(nodeIdentifier, that.nodeIdentifier) && Objects.equals(bucketName, that.bucketName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serviceType, nodeIdentifier, bucketName);
  }
}

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

package com.couchbase.client.core.service;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.TracingIdentifiers;

import static java.util.Objects.requireNonNull;

/**
 * Describes the types of services available in a couchbase cluster.
 *
 * @since 1.0.0
 */
public enum ServiceType {

  /**
   * The Key/Value Service ("kv").
   */
  KV(ServiceScope.BUCKET, TracingIdentifiers.SERVICE_KV),

  /**
   * The Query Service ("n1ql").
   */
  QUERY(ServiceScope.CLUSTER, TracingIdentifiers.SERVICE_QUERY),

  /**
   * The Analytics Service.
   */
  ANALYTICS(ServiceScope.CLUSTER, TracingIdentifiers.SERVICE_ANALYTICS),

  /**
   * The Search Service ("fts").
   */
  SEARCH(ServiceScope.CLUSTER, TracingIdentifiers.SERVICE_SEARCH),

  /**
   * The View Service.
   */
  VIEWS(ServiceScope.CLUSTER, TracingIdentifiers.SERVICE_VIEWS),

  /**
   * The Cluster Manager service ("ns server")
   */
  MANAGER(ServiceScope.CLUSTER, TracingIdentifiers.SERVICE_MGMT, "mgmt"),

  /**
   * The Eventing (function) service.
   */
  EVENTING(ServiceScope.CLUSTER, TracingIdentifiers.SERVICE_EVENTING),

  /**
   * The Backup service
   */
  @Stability.Volatile
  BACKUP(ServiceScope.CLUSTER, TracingIdentifiers.SERVICE_BACKUP),
  ;

  private final ServiceScope scope;
  private final String id;
  private final String ident;

  ServiceType(ServiceScope scope, String id) {
    this(scope, id, id); // ident is same as ID
  }

  ServiceType(ServiceScope scope, String id, String ident) {
    this.scope = requireNonNull(scope);
    this.id = requireNonNull(id);
    this.ident = requireNonNull(ident);
  }

  public ServiceScope scope() {
    return scope;
  }

  /**
   * Like {@link #id()}, but refers to {@link #MANAGER} by the old name {@code "mgmt"}
   * instead of the new standard, {@value TracingIdentifiers#SERVICE_MGMT}.
   *
   * @deprecated New code should favor {@link #id()}.
   */
  @Deprecated
  public String ident() {
    return ident;
  }

  /**
   * Returns the "Service Identifier" specified in SDK RFC-67 "Extended SDK Observability".
   * <p>
   * This value is also known as the service's "tracing identifier."
   */
  public String id() {
    return id;
  }
}

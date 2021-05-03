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

/**
 * Describes the types of services available in a couchbase cluster.
 *
 * @since 1.0.0
 */
public enum ServiceType {

  /**
   * The Key/Value Service ("kv").
   */
  KV(ServiceScope.BUCKET, "kv"),

  /**
   * The Query Service ("n1ql").
   */
  QUERY(ServiceScope.CLUSTER, "query"),

  /**
   * The Analytics Service.
   */
  ANALYTICS(ServiceScope.CLUSTER, "analytics"),

  /**
   * The Search Service ("fts").
   */
  SEARCH(ServiceScope.CLUSTER, "search"),

  /**
   * The View Service.
   */
  VIEWS(ServiceScope.CLUSTER, "views"),

  /**
   * The Cluster Manager service ("ns server")
   */
  MANAGER(ServiceScope.CLUSTER, "mgmt"),

  /**
   * The Eventing (function) service.
   */
  EVENTING(ServiceScope.CLUSTER, "eventing");

  private final ServiceScope scope;
  private final String ident;

  ServiceType(ServiceScope scope, String ident) {
    this.scope = scope;
    this.ident = ident;
  }

  public ServiceScope scope() {
    return scope;
  }

  public String ident() {
    return ident;
  }

}


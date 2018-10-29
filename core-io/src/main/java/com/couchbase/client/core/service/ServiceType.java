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
  KV,

  /**
   * The Query Service ("n1ql").
   */
  QERY,

  /**
   * The Analytics Service.
   */
  ANALYTICS,

  /**
   * The Search Service ("fts").
   */
  SEARCH,

  /**
   * The View Service.
   */
  VIEWS,

  /**
   * The Config Service ("cluster manager").
   */
  CONFIG,

}

/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.core.cnc;

/**
 * Holds static tracing identifiers throughout the SDK.
 */
public class TracingIdentifiers {

  /**
   * This class cannot be instantiated.
   */
  private TracingIdentifiers() {}

  /**
   * A common name for the dispatch span that implementations should use.
   */
  public static final String SPAN_DISPATCH = "cb.dispatch_to_server";

  /**
   * A common name for the value encode span that implementations should use.
   */
  public static final String SPAN_REQUEST_ENCODING = "cb.request_encoding";

  /**
   * The identifier commonly used to identify the kv service.
   */
  public static final String SERVICE_KV = "kv";

  /**
   * The identifier commonly used to identify the query service.
   */
  public static final String SERVICE_QUERY = "query";

  /**
   * The identifier commonly used to identify the search service.
   */
  public static final String SERVICE_SEARCH = "search";

  /**
   * The identifier commonly used to identify the view service.
   */
  public static final String SERVICE_VIEWS = "views";

  /**
   * The identifier commonly used to identify the analytics service.
   */
  public static final String SERVICE_ANALYTICS = "analytics";

  public static final String SPAN_REQUEST_QUERY = "cb.query";

  public static final String SPAN_REQUEST_ANALYTICS = "cb.analytics";

  public static final String SPAN_REQUEST_SEARCH = "cb.search";

  public static final String SPAN_REQUEST_VIEWS = "cb.views";

  public static final String SPAN_REQUEST_KV_GET = "cb.get";

  public static final String SPAN_REQUEST_KV_GET_REPLICA = "cb.get_replica";

  public static final String SPAN_REQUEST_KV_UPSERT = "cb.upsert";

  public static final String SPAN_REQUEST_KV_REPLACE = "cb.replace";

  public static final String SPAN_REQUEST_KV_INSERT = "cb.insert";

  public static final String SPAN_REQUEST_KV_REMOVE = "cb.remove";

  public static final String SPAN_REQUEST_KV_GAL = "cb.get_and_lock";

  public static final String SPAN_REQUEST_KV_GAT = "cb.get_and_touch";

  public static final String SPAN_REQUEST_KV_EXISTS = "cb.exists";

  public static final String SPAN_REQUEST_KV_TOUCH = "cb.touch";

  public static final String SPAN_REQUEST_KV_UNLOCK = "cb.unlock";

  public static final String SPAN_REQUEST_KV_LOOKUP_IN = "cb.lookup_in";

  public static final String SPAN_REQUEST_KV_MUTATE_IN = "cb.mutate_in";

  public static final String SPAN_REQUEST_KV_APPEND = "cb.append";

  public static final String SPAN_REQUEST_KV_PREPEND = "cb.prepend";

  public static final String SPAN_REQUEST_KV_INCREMENT = "cb.increment";

  public static final String SPAN_REQUEST_KV_DECREMENT = "cb.decrement";

  public static final String SPAN_REQUEST_KV_OBSERVE = "cb.observe";

  public static final String SPAN_REQUEST_MANAGER_ANALYTICS = "cb.manager_analytics";

  public static final String SPAN_REQUEST_MANAGER_QUERY = "cb.manager_query";

  public static final String SPAN_REQUEST_MANAGER_BUCKETS = "cb.manager_bucket";

  public static final String SPAN_REQUEST_MANAGER_COLLECTIONS = "cb.manager_collection";

  public static final String SPAN_REQUEST_MANAGER_SEARCH = "cb.manager_search";

  public static final String SPAN_REQUEST_MANAGER_USERS = "cb.manager_user";

  public static final String SPAN_REQUEST_MANAGER_VIEWS = "cb.manager_view";

}

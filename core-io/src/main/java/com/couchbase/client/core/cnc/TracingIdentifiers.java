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
  public static final String SPAN_DISPATCH = "dispatch_to_server";

  /**
   * A common name for the value encode span that implementations should use.
   */
  public static final String SPAN_REQUEST_ENCODING = "request_encoding";

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

  public static final String SPAN_REQUEST_QUERY = "query";

  public static final String SPAN_REQUEST_ANALYTICS = "analytics";

  public static final String SPAN_REQUEST_SEARCH = "search";

  public static final String SPAN_REQUEST_VIEWS = "views";

  public static final String SPAN_REQUEST_KV_GET = "get";

  public static final String SPAN_REQUEST_KV_GET_REPLICA = "get_replica";

  public static final String SPAN_REQUEST_KV_UPSERT = "upsert";

  public static final String SPAN_REQUEST_KV_REPLACE = "replace";

  public static final String SPAN_REQUEST_KV_INSERT = "insert";

  public static final String SPAN_REQUEST_KV_REMOVE = "remove";

  public static final String SPAN_REQUEST_KV_GAL = "get_and_lock";

  public static final String SPAN_REQUEST_KV_GAT = "get_and_touch";

  public static final String SPAN_REQUEST_KV_EXISTS = "exists";

  public static final String SPAN_REQUEST_KV_TOUCH = "touch";

  public static final String SPAN_REQUEST_KV_UNLOCK = "unlock";

  public static final String SPAN_REQUEST_KV_LOOKUP_IN = "lookup_in";

  public static final String SPAN_REQUEST_KV_MUTATE_IN = "mutate_in";

  public static final String SPAN_REQUEST_KV_APPEND = "append";

  public static final String SPAN_REQUEST_KV_PREPEND = "prepend";

  public static final String SPAN_REQUEST_KV_INCREMENT = "increment";

  public static final String SPAN_REQUEST_KV_DECREMENT = "decrement";

  public static final String SPAN_REQUEST_KV_OBSERVE = "observe";

  public static final String SPAN_REQUEST_MANAGER_ANALYTICS = "manager_analytics";

  public static final String SPAN_REQUEST_MANAGER_QUERY = "manager_query";

  public static final String SPAN_REQUEST_MANAGER_BUCKETS = "manager_buckets";

  public static final String SPAN_REQUEST_MANAGER_COLLECTIONS = "manager_collections";

  public static final String SPAN_REQUEST_MANAGER_SEARCH = "manager_search";

  public static final String SPAN_REQUEST_MANAGER_USERS = "manager_user";

  public static final String SPAN_REQUEST_MANAGER_VIEWS = "manager_view";

  public static final String ATTR_SYSTEM = "db.system";

  public static final String ATTR_NAME = "db.name";

  public static final String ATTR_STATEMENT = "db.statement";

  public static final String ATTR_OPERATION = "db.operation";

  public static final String ATTR_SYSTEM_COUCHBASE = "couchbase";

  public static final String ATTR_NET_TRANSPORT = "net.transport";

  public static final String ATTR_NET_TRANSPORT_TCP = "IP.TCP";

  public static final String ATTR_LOCAL_ID = "db.couchbase.local_id";

  public static final String ATTR_LOCAL_HOSTNAME = "net.host.name";

  public static final String ATTR_LOCAL_PORT = "net.host.port";

  public static final String ATTR_REMOTE_HOSTNAME = "net.peer.name";

  public static final String ATTR_REMOTE_PORT = "net.peer.port";

  public static final String ATTR_OPERATION_ID = "db.couchbase.operation_id";

  public static final String ATTR_SERVER_DURATION = "db.couchbase.server_duration";

  public static final String ATTR_SERVICE = "db.couchbase.service";

  public static final String ATTR_COLLECTION = "db.couchbase.collection";

  public static final String ATTR_SCOPE = "db.couchbase.scope";


}

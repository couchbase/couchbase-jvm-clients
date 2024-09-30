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

import com.couchbase.client.core.annotation.Stability;

/**
 * Holds static tracing and metrics identifiers throughout the SDK.
 */
@Stability.Internal
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
   * The identifier commonly used to identify the management service.
   */
  public static final String SERVICE_MGMT = "management";

  /**
   * The identifier commonly used to identify the analytics service.
   */
  public static final String SERVICE_ANALYTICS = "analytics";

  /**
   * The identifier commonly used to identify the eventing service.
   */
  public static final String SERVICE_EVENTING = "eventing";

  /**
   * The identifier commonly used to identify the backup service.
   */
  public static final String SERVICE_BACKUP = "backup";

  /**
   * The identifier commonly used to identify the transactions "service".
   */
  public static final String SERVICE_TRANSACTIONS = "transactions";

  /**
   * Used only in situations where due to likely internal error we don't have a known service.
   */
  public static final String SERVICE_UNKNOWN = "unknown";

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

  public static final String SPAN_REQUEST_KV_GET_AND_LOCK = "get_and_lock";

  public static final String SPAN_REQUEST_KV_GET_AND_TOUCH = "get_and_touch";

  public static final String SPAN_REQUEST_KV_EXISTS = "exists";

  public static final String SPAN_REQUEST_KV_TOUCH = "touch";

  public static final String SPAN_REQUEST_KV_UNLOCK = "unlock";

  public static final String SPAN_REQUEST_KV_LOOKUP_IN = "lookup_in";

  public static final String SPAN_REQUEST_KV_LOOKUP_IN_REPLICA = "lookup_in_replica";

  public static final String SPAN_REQUEST_KV_MUTATE_IN = "mutate_in";

  public static final String SPAN_REQUEST_KV_APPEND = "append";

  public static final String SPAN_REQUEST_KV_PREPEND = "prepend";

  public static final String SPAN_REQUEST_KV_INCREMENT = "increment";

  public static final String SPAN_REQUEST_KV_DECREMENT = "decrement";

  public static final String SPAN_REQUEST_KV_OBSERVE = "observe";

  public static final String SPAN_REQUEST_KV_RANGE_SCAN_CREATE = "range_scan_create";

  public static final String SPAN_REQUEST_KV_RANGE_SCAN_CONTINUE = "range_scan_continue";

  public static final String SPAN_REQUEST_KV_RANGE_SCAN_CANCEL = "range_scan_cancel";

  public static final String SPAN_GET_ALL_REPLICAS = "get_all_replicas";

  public static final String SPAN_GET_ANY_REPLICA = "get_any_replica";

  public static final String SPAN_LOOKUP_IN_ALL_REPLICAS = "lookup_in_all_replicas";

  public static final String SPAN_LOOKUP_IN_ANY_REPLICA = "lookup_in_any_replica";

  public static final String SPAN_REQUEST_MA_CONNECT_LINK = "manager_analytics_connect_link";
  public static final String SPAN_REQUEST_MA_CREATE_DATASET = "manager_analytics_create_dataset";
  public static final String SPAN_REQUEST_MA_CREATE_DATAVERSE = "manager_analytics_create_dataverse";
  public static final String SPAN_REQUEST_MA_CREATE_INDEX = "manager_analytics_create_index";
  public static final String SPAN_REQUEST_MA_DISCONNECT_LINK = "manager_analytics_disconnect_link";
  public static final String SPAN_REQUEST_MA_DROP_DATASET = "manager_analytics_drop_dataset";
  public static final String SPAN_REQUEST_MA_DROP_DATAVERSE = "manager_analytics_drop_dataverse";
  public static final String SPAN_REQUEST_MA_DROP_INDEX = "manager_analytics_drop_index";
  public static final String SPAN_REQUEST_MA_GET_ALL_DATASETS = "manager_analytics_get_all_datasets";
  public static final String SPAN_REQUEST_MA_GET_ALL_INDEXES = "manager_analytics_get_all_indexes";
  public static final String SPAN_REQUEST_MA_GET_ALL_DATAVERSES = "manager_analytics_get_all_dataverses";
  public static final String SPAN_REQUEST_MA_GET_PENDING_MUTATIONS = "manager_analytics_get_pending_mutations";
  public static final String SPAN_REQUEST_MA_GET_ALL_LINKS = "manager_analytics_get_all_links";
  public static final String SPAN_REQUEST_MA_CREATE_LINK = "manager_analytics_create_link";
  public static final String SPAN_REQUEST_MA_REPLACE_LINK = "manager_analytics_replace_link";
  public static final String SPAN_REQUEST_MA_DROP_LINK = "manager_analytics_drop_link";


  public static final String SPAN_REQUEST_MQ_BUILD_DEFERRED_INDEXES = "manager_query_build_deferred_indexes";
  public static final String SPAN_REQUEST_MQ_CREATE_INDEX = "manager_query_create_index";
  public static final String SPAN_REQUEST_MQ_CREATE_PRIMARY_INDEX = "manager_query_create_primary_index";
  public static final String SPAN_REQUEST_MQ_DROP_INDEX = "manager_query_drop_index";
  public static final String SPAN_REQUEST_MQ_DROP_PRIMARY_INDEX = "manager_query_drop_primary_index";
  public static final String SPAN_REQUEST_MQ_GET_ALL_INDEXES = "manager_query_get_all_indexes";
  public static final String SPAN_REQUEST_MQ_WATCH_INDEXES = "manager_query_watch_indexes";

  public static final String SPAN_REQUEST_MB_CREATE_BUCKET = "manager_buckets_create_bucket";
  public static final String SPAN_REQUEST_MB_DROP_BUCKET = "manager_buckets_drop_bucket";
  public static final String SPAN_REQUEST_MB_FLUSH_BUCKET = "manager_buckets_flush_bucket";
  public static final String SPAN_REQUEST_MB_GET_ALL_BUCKETS = "manager_buckets_get_all_buckets";
  public static final String SPAN_REQUEST_MB_GET_BUCKET = "manager_buckets_get_bucket";
  public static final String SPAN_REQUEST_MB_UPDATE_BUCKET = "manager_buckets_update_bucket";

  public static final String SPAN_REQUEST_MC_CREATE_COLLECTION = "manager_collections_create_collection";
  public static final String SPAN_REQUEST_MC_UPDATE_COLLECTION = "manager_collections_update_collection";
  public static final String SPAN_REQUEST_MC_CREATE_SCOPE = "manager_collections_create_scope";
  public static final String SPAN_REQUEST_MC_DROP_COLLECTION = "manager_collections_drop_collection";
  public static final String SPAN_REQUEST_MC_DROP_SCOCPE = "manager_collections_drop_scope";
  public static final String SPAN_REQUEST_MC_GET_ALL_SCOPES = "manager_collections_get_all_scopes";

  public static final String SPAN_REQUEST_MS_ALLOW_QUERYING = "manager_search_allow_querying";
  public static final String SPAN_REQUEST_MS_ANALYZE_DOCUMENT = "manager_search_analyze_document";
  public static final String SPAN_REQUEST_MS_DISALLOW_QUERYING = "manager_search_disallow_querying";
  public static final String SPAN_REQUEST_MS_DROP_INDEX = "manager_search_drop_index";
  public static final String SPAN_REQUEST_MS_FREEZE_PLAN = "manager_search_freeze_plan";
  public static final String SPAN_REQUEST_MS_GET_ALL_INDEXES = "manager_search_get_all_indexes";
  public static final String SPAN_REQUEST_MS_GET_INDEX = "manager_search_get_index";
  public static final String SPAN_REQUEST_MS_GET_IDX_DOC_COUNT = "manager_search_get_indexed_documents_count";
  public static final String SPAN_REQUEST_MS_PAUSE_INGEST = "manager_search_pause_ingest";
  public static final String SPAN_REQUEST_MS_RESUME_INGEST = "manager_search_resume_ingest";
  public static final String SPAN_REQUEST_MS_UNFREEZE_PLAN = "manager_search_unfreeze_plan";
  public static final String SPAN_REQUEST_MS_UPSERT_INDEX = "manager_search_upsert_index";

  public static final String SPAN_REQUEST_MU_DROP_GROUP = "manager_users_drop_group";
  public static final String SPAN_REQUEST_MU_DROP_USER = "manager_users_drop_user";
  public static final String SPAN_REQUEST_MU_GET_ALL_GROUPS = "manager_users_get_all_groups";
  public static final String SPAN_REQUEST_MU_GET_ALL_USERS = "manager_users_get_all_users";
  public static final String SPAN_REQUEST_MU_GET_GROUP = "manager_users_get_group";
  public static final String SPAN_REQUEST_MU_GET_ROLES = "manager_users_get_roles";
  public static final String SPAN_REQUEST_MU_GET_USER = "manager_users_get_user";
  public static final String SPAN_REQUEST_MU_UPSERT_GROUP = "manager_users_upsert_group";
  public static final String SPAN_REQUEST_MU_UPSERT_USER = "manager_users_upsert_user";
  public static final String SPAN_REQUEST_MU_CHANGE_PASSWORD = "manager_users_change_password";

  public static final String SPAN_REQUEST_MV_DROP_DD = "manager_views_drop_design_document";
  public static final String SPAN_REQUEST_MV_GET_ALL_DD = "manager_views_get_all_design_documents";
  public static final String SPAN_REQUEST_MV_GET_DD = "manager_views_get_design_document";
  public static final String SPAN_REQUEST_MV_PUBLISH_DD = "manager_views_publish_design_document";
  public static final String SPAN_REQUEST_MV_UPSERT_DD = "manager_views_upsert_design_document";

  public static final String SPAN_REQUEST_ME_UPSERT = "manager_eventing_upsert_function";
  public static final String SPAN_REQUEST_ME_GET = "manager_eventing_get_function";
  public static final String SPAN_REQUEST_ME_DROP = "manager_eventing_drop_function";
  public static final String SPAN_REQUEST_ME_DEPLOY = "manager_eventing_deploy_function";
  public static final String SPAN_REQUEST_ME_GET_ALL = "manager_eventing_get_all_functions";
  public static final String SPAN_REQUEST_ME_PAUSE = "manager_eventing_pause_function";
  public static final String SPAN_REQUEST_ME_RESUME = "manager_eventing_resume_function";
  public static final String SPAN_REQUEST_ME_UNDEPLOY = "manager_eventing_undeploy_function";
  public static final String SPAN_REQUEST_ME_STATUS = "manager_eventing_functions_status";

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

  public static final String ATTR_DOCUMENT_ID = "db.couchbase.document_id";
  public static final String ATTR_CLUSTER_UUID = "db.couchbase.cluster_uuid";
  public static final String ATTR_CLUSTER_NAME = "db.couchbase.cluster_name";

  public static final String ATTR_TRANSACTION_ID = "db.couchbase.transaction.id";
  public static final String ATTR_TRANSACTION_ATTEMPT_ID = "db.couchbase.transaction.attempt_id";
  public static final String ATTR_TRANSACTION_STATE = "db.couchbase.transaction.state";
  public static final String ATTR_TRANSACTION_AGE = "db.couchbase.transaction.age_ms";
  public static final String ATTR_TRANSACTION_SINGLE_QUERY = "db.couchbase.transaction.single_query";
  public static final String ATTR_TRANSACTION_CLEANUP_CLIENT_ID = "db.couchbase.transactions.cleanup.client_id";
  public static final String ATTR_TRANSACTION_CLEANUP_WINDOW = "db.couchbase.transactions.cleanup.window_ms";
  public static final String ATTR_TRANSACTION_CLEANUP_NUM_ATRS = "db.couchbase.transactions.cleanup.client_atrs";
  public static final String ATTR_TRANSACTION_CLEANUP_NUM_ACTIVE = "db.couchbase.transactions.cleanup.clients_active";
  public static final String ATTR_TRANSACTION_CLEANUP_NUM_EXPIRED = "db.couchbase.transactions.cleanup.clients_expired";
  public static final String ATTR_TRANSACTION_ATR_ENTRIES_COUNT = "db.couchbase.transactions.atr.entries_count";
  public static final String ATTR_TRANSACTION_ATR_ENTRIES_EXPIRED = "db.couchbase.transactions.atr.entries_expired";

  public static final String METER_OPERATIONS = "db.couchbase.operations";
  public static final String METER_TRANSACTION_TOTAL = "db.couchbase.transactions.total";
  public static final String METER_TRANSACTION_ATTEMPTS = "db.couchbase.transactions.attempts";

  public static final String ATTR_DURABILITY = "db.couchbase.durability";

  public static final String ATTR_RETRIES = "db.couchbase.retries";
  public static final String ATTR_OUTCOME = "outcome";
  public static final String TRANSACTION_OP = "transaction";
  public static final String TRANSACTION_OP_ATTEMPT = "transaction_attempt";
  public static final String TRANSACTION_OP_REPLACE = "transaction_replace";
  public static final String TRANSACTION_OP_INSERT = "transaction_insert";
  public static final String TRANSACTION_OP_REMOVE = "transaction_remove";
  public static final String TRANSACTION_OP_GET = "transaction_get";
  public static final String TRANSACTION_OP_GET_REPLICA_FROM_PREFERRED_SERVER_GROUP = "transaction_get_replica_from_preferred_server_group";
  public static final String TRANSACTION_OP_QUERY = "transaction_query";
  public static final String TRANSACTION_OP_INSERT_STAGE = "transaction_insert_stage";
  public static final String TRANSACTION_OP_REPLACE_STAGE = "transaction_replace_stage";
  public static final String TRANSACTION_OP_REMOVE_STAGE = "transaction_remove_stage";
  public static final String TRANSACTION_OP_ATR_PENDING = "transaction_atr_pending";
  public static final String TRANSACTION_OP_ATR_COMMIT = "transaction_atr_commit";
  public static final String TRANSACTION_OP_ATR_COMPLETE = "transaction_atr_complete";
  public static final String TRANSACTION_OP_ATR_ROLLBACK = "transaction_atr_rollback";
  public static final String TRANSACTION_OP_ATR_ABORT = "transaction_atr_abort";
  public static final String TRANSACTION_OP_COMMIT = "transaction_commit";
  public static final String TRANSACTION_OP_ROLLBACK = "transaction_rollback";
  public static final String TRANSACTION_CLEANUP_WINDOW = "transaction_cleanup_window";
  public static final String TRANSACTION_CLEANUP_ATR = "transaction_cleanup_atr";
  public static final String TRANSACTION_CLEANUP = "transaction_cleanup";
  public static final String TRANSACTION_CLEANUP_CLIENT = "transaction_cleanup_client";

}

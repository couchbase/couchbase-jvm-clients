/*
 * Copyright (c) 2025 Couchbase, Inc.
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
package com.couchbase.client.core.cnc.tracing;

import com.couchbase.client.core.annotation.Stability;

@Stability.Internal
public enum TracingAttribute {
  SERVICE,
  BUCKET_NAME,
  SCOPE_NAME,
  COLLECTION_NAME,
  DOCUMENT_ID,
  DURABILITY,
  OUTCOME,
  TRANSACTION_STATE,
  TRANSACTION_AGE,
  SERVER_DURATION,
  OPERATION_ID,
  LOCAL_ID,
  LOCAL_HOSTNAME,
  LOCAL_PORT,
  REMOTE_HOSTNAME,
  REMOTE_PORT,
  PEER_HOSTNAME,
  PEER_PORT,
  RETRIES,
  STATEMENT,
  TRANSACTION_SINGLE_QUERY,
  OPERATION,
  TRANSACTION_ID,
  TRANSACTION_ATTEMPT_ID,
  TRANSACTION_CLEANUP_CLIENT_ID,
  TRANSACTION_CLEANUP_WINDOW,
  SYSTEM,
  NET_TRANSPORT,
  CLUSTER_NAME,
  CLUSTER_UUID,
  TRANSACTION_CLEANUP_NUM_ATRS,
  TRANSACTION_CLEANUP_NUM_ACTIVE,
  TRANSACTION_CLEANUP_NUM_EXPIRED,
  TRANSACTION_ATR_ENTRIES_COUNT,
  TRANSACTION_ATR_ENTRIES_EXPIRED,
}

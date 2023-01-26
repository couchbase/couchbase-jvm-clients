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

package com.couchbase.client.test;

/**
 * Certain capabilities used to figure out if a test can be run or not.
 */
public enum Capabilities {
  /**
   * This cluster is able to perform sync replications.
   */
  SYNC_REPLICATION,
  /**
   * This cluster is able to handle N1QL queries.
   */
  QUERY,
  /**
   * This cluster is able to handle Analytics queries.
   */
  ANALYTICS,
  /**
   * This cluster is able to handle Search queries.
   */
  SEARCH,
  /**
   * This cluster is able to handle Eventing functions.
   */
  EVENTING,
  /**
   * This cluster is running the Backup service.
   */
  BACKUP,
  /**
   * This cluster is able to give us a config without opening a bucket.
   */
  GLOBAL_CONFIG,
  /**
   * This cluster is able to assign users to groups.
   */
  USER_GROUPS,
  /**
   * The cluster has collections enabled.
   */
  COLLECTIONS,
  /**
   * The cluster has views enabled.
   */
  VIEWS,
  /**
   * The cluster can create documents in a deleted state.
   */
  CREATE_AS_DELETED,
  /**
   * The cluster supports specifying a minimum durability level on the bucket.
   */
  BUCKET_MINIMUM_DURABILITY,
  /**
   * The cluster can modify a document without changing its expiry.
   */
  PRESERVE_EXPIRY,
  /**
   * The cluster is using the enterprise edition version
   */
  ENTERPRISE_EDITION,
  /**
   * The cluster supports a Sub-Document instruction to replace a document's body with an xattr
   */
  SUBDOC_REPLACE_BODY_WITH_XATTR,
  /**
   * The cluster supports the Sub-Document ReviveDocument flag to turn a tombstone into a regular document, preserving xattrs.
   */
  SUBDOC_REVIVE_DOCUMENT,
  /**
   * The cluster supports rate limiting.
   */
  RATE_LIMITING,
  /**
   * The cluster supports setting the storage backend (e.g. Magma).
   */
  STORAGE_BACKEND,
  /**
   * The cluster can modify a document with Queries without changing its expiry.
   */
  QUERY_PRESERVE_EXPIRY,
  /**
   * Supports KV Range Scan operations.
   */
  RANGE_SCAN,
  /**
   * JVMCBC-1187: There is no cluster cap for Protostellar currently, but will be ultimately.
   */
  PROTOSTELLAR
}

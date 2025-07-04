/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.core.topology;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonValue;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public enum BucketCapability {
  CBHELLO("cbhello"),
  TOUCH("touch"),
  COUCHAPI("couchapi"),
  CCCP("cccp"),
  XDCR_CHECKPOINTING("xdcrCheckpointing"),
  NODES_EXT("nodesExt"),
  DCP("dcp"),
  XATTR("xattr"),
  SNAPPY("snappy"),
  COLLECTIONS("collections"),
  DURABLE_WRITE("durableWrite"),
  CREATE_AS_DELETED("tombstonedUserXAttrs"),
  SUBDOC_REPLACE_BODY_WITH_XATTR("subdoc.ReplaceBodyWithXattr"),
  SUBDOC_REVIVE_DOCUMENT("subdoc.ReviveDocument"),
  DCP_IGNORE_PURGED_TOMBSTONES("dcp.IgnorePurgedTombstones"),
  RANGE_SCAN("rangeScan"),
  SUBDOC_READ_REPLICA("subdoc.ReplicaRead"),
  NON_DEDUPED_HISTORY("nonDedupedHistory"),
  SUBDOC_BINARY_XATTR("subdoc.BinaryXattr"),
  // Added in 8.0 (MB-66949)
  SUBDOC_ACCESS_DELETED("subdoc.AccessDeleted")
  ;

  private final String wireName;

  BucketCapability(String wireName) {
    this.wireName = requireNonNull(wireName);
  }

  @JsonValue
  public String wireName() {
    return wireName;
  }
}

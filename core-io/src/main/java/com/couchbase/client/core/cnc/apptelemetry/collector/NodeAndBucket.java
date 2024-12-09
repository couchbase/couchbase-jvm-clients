/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.apptelemetry.collector;

import com.couchbase.client.core.topology.NodeIdentifier;
import reactor.util.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

class NodeAndBucket {
  public final NodeIdentifier nodeId;
  public final @Nullable String bucket;

  NodeAndBucket(
    NodeIdentifier nodeId,
    @Nullable String bucket
  ) {
    this.nodeId = requireNonNull(nodeId);
    this.bucket = bucket;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NodeAndBucket that = (NodeAndBucket) o;
    return Objects.equals(nodeId, that.nodeId) && Objects.equals(bucket, that.bucket);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeId, bucket);
  }

  @Override
  public String toString() {
    return "NodeAndBucket{" +
      "nodeId='" + nodeId + '\'' +
      ", bucket='" + bucket + '\'' +
      '}';
  }

  void writeTo(Map<String, String> map) {
    String canonicalHost = nodeId.canonical().host();
    String usedHost = nodeId.hostForNetworkConnections();
    map.put("node", canonicalHost);
    if (!usedHost.equals(canonicalHost)) {
      map.put("alt_node", usedHost);
    }
    if (bucket != null) {
      map.put("bucket", bucket);
    }
  }
}

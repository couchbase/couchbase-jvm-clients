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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class ClusterIdentifier {
  private final String clusterUuid;
  private final String clusterName;
  private final ClusterType clusterType;

  ClusterIdentifier(String clusterUuid, String clusterName, ClusterType product) {
    this.clusterUuid = requireNonNull(clusterUuid);
    this.clusterName = requireNonNull(clusterName);
    this.clusterType = requireNonNull(product);
  }

  public static @Nullable ClusterIdentifier parse(ObjectNode config) {
    JsonNode clusterUuid = config.path("clusterUUID");
    JsonNode clusterName = config.path("clusterName");
    if (clusterUuid.isMissingNode() || clusterName.isMissingNode()) {
      return null;
    }

    JsonNode prodName = config.path("prodName"); // field added in Couchbase Server 8.0.
    ClusterType type = ClusterType.of(prodName.textValue());
    return new ClusterIdentifier(clusterUuid.asText(), clusterName.asText(), type);
  }

  public String clusterUuid() {
    return clusterUuid;
  }

  public String clusterName() {
    return clusterName;
  }

  public ClusterType clusterType() {
    return clusterType;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    ClusterIdentifier that = (ClusterIdentifier) o;
    return Objects.equals(clusterUuid, that.clusterUuid)
            && Objects.equals(clusterName, that.clusterName)
            && Objects.equals(clusterType, that.clusterType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterUuid, clusterName, clusterType);
  }

  @Override
  public String toString() {
    return "ClusterIdent{" +
      "clusterUuid='" + clusterUuid + '\'' +
      ", clusterName='" + redactMeta(clusterName) + '\'' +
      ", clusterType='" + clusterType + '\'' +
      '}';
  }
}

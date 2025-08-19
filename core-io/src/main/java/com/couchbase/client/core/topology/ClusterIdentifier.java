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
  private final String product;

  ClusterIdentifier(String clusterUuid, String clusterName, String product) {
    this.clusterUuid = requireNonNull(clusterUuid);
    this.clusterName = requireNonNull(clusterName);
    this.product = requireNonNull(product);
  }

  public static @Nullable ClusterIdentifier parse(ObjectNode config) {
    // Cluster UUID and name were added in Couchbase Server 7.6.4.
    JsonNode clusterUuid = config.path("clusterUUID");
    JsonNode clusterName = config.path("clusterName");
    if (clusterUuid.isMissingNode() || clusterName.isMissingNode()) {
      return null;
    }

    // Field "prod" added in Couchbase Server 8.0 (value = "server") / Enterprise Analytics 1.0 (value = "analytics").
    // Assume anything that doesn't set it is an older version of Couchbase Server.
    String prod = config.path("prod").asText("server");
    return new ClusterIdentifier(clusterUuid.asText(), clusterName.asText(), prod);
  }

  public String clusterUuid() {
    return clusterUuid;
  }

  public String clusterName() {
    return clusterName;
  }

  public String product() {
    return product;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    ClusterIdentifier that = (ClusterIdentifier) o;
    return Objects.equals(clusterUuid, that.clusterUuid)
            && Objects.equals(clusterName, that.clusterName)
            && Objects.equals(product, that.product);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterUuid, clusterName, product);
  }

  @Override
  public String toString() {
    return "ClusterIdent{" +
      "clusterUuid='" + clusterUuid + '\'' +
      ", clusterName='" + redactMeta(clusterName) + '\'' +
      ", product='" + product + '\'' +
      '}';
  }
}

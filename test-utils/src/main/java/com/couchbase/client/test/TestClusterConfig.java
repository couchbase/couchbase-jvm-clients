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

import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * This configuration is populated from the cluster container and represents the
 * settings that can be used from the tests for bootstrapping their own code.
 *
 * @since 2.0.0
 */
public class TestClusterConfig {

  private final String bucketname;
  private final String adminUsername;
  private final String adminPassword;
  private final List<TestNodeConfig> nodes;
  private final int numReplicas;
  private final Optional<List<X509Certificate>> clusterCerts;
  private final Set<Capabilities> capabilities;
  private final ClusterVersion clusterVersion;
  private final boolean runWithTLS;

  TestClusterConfig(String bucketname, String adminUsername, String adminPassword,
                    List<TestNodeConfig> nodes, int numReplicas,
                    Optional<List<X509Certificate>> clusterCerts,
                    Set<Capabilities> capabilities, ClusterVersion clusterVersion, boolean runWithTLS) {
    this.bucketname = bucketname;
    this.adminUsername = adminUsername;
    this.adminPassword = adminPassword;
    this.nodes = nodes;
    this.numReplicas = numReplicas;
    this.clusterCerts = clusterCerts;
    this.capabilities = capabilities;
    this.clusterVersion = clusterVersion;
    this.runWithTLS = runWithTLS;
  }

  public String bucketname() {
    return bucketname;
  }

  public String adminUsername() {
    return adminUsername;
  }

  public String adminPassword() {
    return adminPassword;
  }

  public List<TestNodeConfig> nodes() {
    return nodes;
  }

  public int numReplicas() {
    return numReplicas;
  }

  public Set<Capabilities> capabilities() {
    return capabilities;
  }

  public Optional<List<X509Certificate>> clusterCerts() {
    return clusterCerts;
  }

  public ClusterVersion clusterVersion() {
    return clusterVersion;
  }

  public boolean runWithTLS() {
    return runWithTLS;
  }

  public boolean isProtostellar() {
    return nodes.stream().anyMatch(node -> node.protostellarPort().isPresent());
  }

  /**
   * Finds the first node with a given service enabled in the config.
   *
   * <p>This method can be used to find bootstrap nodes and similar.</p>
   *
   * @param service the service to find.
   * @return a node config if found, empty otherwise.
   */
  public Optional<TestNodeConfig> firstNodeWith(Services service) {
    return nodes.stream().filter(n -> n.ports().containsKey(service)).findFirst();
  }

  @Override
  public String toString() {
    return "TestClusterConfig{" +
      "bucketname='" + bucketname + '\'' +
      ", adminUsername='" + adminUsername + '\'' +
      ", adminPassword='" + adminPassword + '\'' +
      ", nodes=" + nodes +
      ", numReplicas=" + numReplicas +
      ", clusterCerts=" + clusterCerts +
      ", capabilities=" + capabilities +
      ", version=" + clusterVersion +
      '}';
  }
}

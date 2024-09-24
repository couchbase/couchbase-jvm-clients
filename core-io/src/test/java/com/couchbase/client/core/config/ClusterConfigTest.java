/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.core.config;

import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClusterConfigTest {

  private static PortInfo minimalPortInfo(String host) {
    return new PortInfo(mapOf("mgmt", 8091), host, emptyMap(), null);
  }

  private static NodeInfo minimalNodeInfo(String host, int port) {
    return new NodeInfo("", new HostAndPort(host, port).format(), emptyMap(), emptyMap());
  }

  @Test
  void returnsEmptyAllNodeAddresses() {
    ClusterConfig config = new ClusterConfig();
    assertTrue(config.allNodeAddresses().isEmpty());
  }

  @Test
  void allNodeAddressesWithOnlyGlobalConfig() {
    ClusterConfig config = new ClusterConfig();

    GlobalConfig gc = mock(GlobalConfig.class);
    when(gc.portInfos()).thenReturn(listOf(
      minimalPortInfo("hostname1"),
      minimalPortInfo("hostname2")
    ));
    config.setGlobalConfig(gc);

    Set<String> expected = setOf(
      "hostname1",
      "hostname2"
    );

    assertEquals(expected, config.allNodeAddresses());
  }

  @Test
  void allNodeAddressesWithOnlyBucketConfig() {
    ClusterConfig config = new ClusterConfig();

    BucketConfig bc = mock(BucketConfig.class);
    when(bc.name()).thenReturn("bucket-name");
    when(bc.nodes()).thenReturn(listOf(
      minimalNodeInfo("hostname1", 11210),
      minimalNodeInfo("hostname2", 11210),
      minimalNodeInfo("hostname3", 11210)
    ));
    config.setBucketConfig(bc);

    Set<String> expected = setOf(
      "hostname1",
      "hostname2",
      "hostname3"
    );

    assertEquals(expected, config.allNodeAddresses());
  }

  @Test
  void allNodeAddressesWithGlobalAndBucketConfigs() {
    ClusterConfig config = new ClusterConfig();

    GlobalConfig gc = mock(GlobalConfig.class);
    when(gc.portInfos()).thenReturn(Arrays.asList(
      minimalPortInfo("hostname1"),
      minimalPortInfo("hostname2")
    ));
    config.setGlobalConfig(gc);

    BucketConfig bc = mock(BucketConfig.class);
    when(bc.name()).thenReturn("bucket-name");
    when(bc.nodes()).thenReturn(listOf(
      minimalNodeInfo("hostname1", 11210),
      minimalNodeInfo("hostname2", 11210),
      minimalNodeInfo("hostname3", 11210)
    ));
    config.setBucketConfig(bc);

    Set<String> expected = setOf(
      "hostname1",
      "hostname2",
      "hostname3"
    );

    assertEquals(expected, config.allNodeAddresses());
  }

}

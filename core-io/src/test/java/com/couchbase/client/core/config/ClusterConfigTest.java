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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClusterConfigTest {

  @Test
  void returnsEmptyAllNodeAddresses() {
    ClusterConfig config = new ClusterConfig();
    assertTrue(config.allNodeAddresses().isEmpty());
  }

  @Test
  void allNodeAddressesWithOnlyGlobalConfig() {
    ClusterConfig config = new ClusterConfig();

    GlobalConfig gc = mock(GlobalConfig.class);
    when(gc.portInfos()).thenReturn(Arrays.asList(
      new PortInfo(Collections.emptyMap(), "hostname1", Collections.emptyMap()),
      new PortInfo(Collections.emptyMap(), "hostname2", Collections.emptyMap())
    ));
    config.setGlobalConfig(gc);

    Set<String> expected = new HashSet<>();
    expected.add("hostname1");
    expected.add("hostname2");

    assertEquals(expected, config.allNodeAddresses());
  }

  @Test
  void allNodeAddressesWithOnlyBucketConfig() {
    ClusterConfig config = new ClusterConfig();

    BucketConfig bc = mock(BucketConfig.class);
    when(bc.name()).thenReturn("bucket-name");
    when(bc.nodes()).thenReturn(Arrays.asList(
      new NodeInfo("", "hostname1:11210", Collections.emptyMap(), Collections.emptyMap()),
      new NodeInfo("", "hostname2:11210", Collections.emptyMap(), Collections.emptyMap()),
      new NodeInfo("", "hostname3:11210", Collections.emptyMap(), Collections.emptyMap())
    ));
    config.setBucketConfig(bc);

    Set<String> expected = new HashSet<>();
    expected.add("hostname1");
    expected.add("hostname2");
    expected.add("hostname3");

    assertEquals(expected, config.allNodeAddresses());
  }

  @Test
  void allNodeAddressesWithGlobalAndBucketConfigs() {
    ClusterConfig config = new ClusterConfig();

    GlobalConfig gc = mock(GlobalConfig.class);
    when(gc.portInfos()).thenReturn(Arrays.asList(
      new PortInfo(Collections.emptyMap(), "hostname1", Collections.emptyMap()),
      new PortInfo(Collections.emptyMap(), "hostname2", Collections.emptyMap())
    ));
    config.setGlobalConfig(gc);

    BucketConfig bc = mock(BucketConfig.class);
    when(bc.name()).thenReturn("bucket-name");
    when(bc.nodes()).thenReturn(Arrays.asList(
      new NodeInfo("", "hostname1:11210", Collections.emptyMap(), Collections.emptyMap()),
      new NodeInfo("", "hostname2:11210", Collections.emptyMap(), Collections.emptyMap()),
      new NodeInfo("", "hostname3:11210", Collections.emptyMap(), Collections.emptyMap())
    ));
    config.setBucketConfig(bc);

    Set<String> expected = new HashSet<>();
    expected.add("hostname1");
    expected.add("hostname2");
    expected.add("hostname3");

    assertEquals(expected, config.allNodeAddresses());
  }

}

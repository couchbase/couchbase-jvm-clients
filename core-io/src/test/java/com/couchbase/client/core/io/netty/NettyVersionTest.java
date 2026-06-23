/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty;

import com.couchbase.client.core.deps.io.netty.util.Version;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.TreeSet;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class NettyVersionTest {
  /**
   * Assert that we relocated and merged the `io.netty.versions.properties`
   * files from the repackaged Netty component JARs.
   * <p>
   * We relocated these resources so they don't interfere with a user's vanilla Netty.
   */
  @Test
  void canReadRepackagedNettyComponentVersions() {
    // Listing all components alerts us if a new Netty dependency
    // is added accidentally. If this tests becomes a maintenance burden,
    // a more permissive alternative would be to assert Versions.identify()
    // returns a map with more than one entry.
    assertEquals(
      new TreeSet<>(setOf(
        "netty-buffer",
        "netty-codec-base",
        "netty-codec-compression",
        "netty-codec-http",
        "netty-common",
        "netty-handler",
        "netty-resolver",
        "netty-transport",
        "netty-transport-classes-epoll",
        "netty-transport-classes-kqueue",
        "netty-transport-native-unix-common",

        // and these via Protostellar's gRPC
        "netty-codec-http2",
        "netty-handler-proxy",
        "netty-codec-socks"
      )),
      new TreeSet<>(Version.identify().keySet())
    );
  }

  @Test
  void nettyComponentsHaveSameVersion() {
    Set<String> distinctVersions = Version.identify().values().stream()
      .map(Version::artifactVersion)
      .collect(toSet());

    assertEquals(
      1,
      distinctVersions.size(),
      "Expected single Netty component version, but got: " + distinctVersions + "; full info: " + Version.identify());
  }
}


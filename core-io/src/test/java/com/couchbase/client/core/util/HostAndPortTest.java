/*
 * Copyright 2022 Couchbase, Inc.
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

package com.couchbase.client.core.util;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.*;

class HostAndPortTest {
  @Test
  void ipv6LiteralsAreCanonicalized() throws Exception {
    assertEquals("0:0:0:0:0:0:0:1", new HostAndPort("::1", 0).host());
    assertEquals("0:0:0:0:0:0:0:a", new HostAndPort("::A", 0).host());

    // Strips brackets from IPv6 hosts, if present
    assertEquals("0:0:0:0:0:0:0:1", new HostAndPort("[::1]", 0).host());
  }

  @Test
  void equalsUsesCanonicalHost() throws Exception {
    assertEquals(new HostAndPort("0:0:0:0:0:0:0:1", 0), new HostAndPort("::1", 0));
    assertEquals(new HostAndPort("0:0:0:0:0:0:0:a", 0), new HostAndPort("::A", 0));
  }

  @Test
  void equalsUsesUnresolvedNames() throws Exception {
    assertNotEquals(new HostAndPort("localhost", 0), new HostAndPort("127.0.0.1", 0));
    assertNotEquals(new HostAndPort("localhost", 0), new HostAndPort("::1", 0));
  }

  @Test
  void format() throws Exception {
    assertEquals("127.0.0.1:12345", new HostAndPort("127.0.0.1", 12345).format());
    assertEquals("[0:0:0:0:0:0:0:1]:12345", new HostAndPort("0:0:0:0:0:0:0:1", 12345).format());
    assertEquals("[0:0:0:0:0:0:0:1]:12345", new HostAndPort("[::1]", 12345).format());
    assertEquals("example.com:12345", new HostAndPort("example.com", 12345).format());
  }

}

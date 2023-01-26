/*
 * Copyright (c) 2019 Couchbase, Inc.
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

import java.util.Map;
import java.util.Optional;

public class TestNodeConfig {

  private final String hostname;
  private final Map<Services, Integer> ports;
  private final boolean dns;
  private final Optional<Integer> protostellarPort;

  public TestNodeConfig(String hostname, Map<Services, Integer> ports, boolean dns, Optional<Integer> protostellarPort) {
    this.hostname = hostname;
    this.ports = ports;
    this.dns = dns;
    this.protostellarPort = protostellarPort;
  }

  public String hostname() {
    return hostname;
  }

  public Map<Services, Integer> ports() {
    return ports;
  }

  public boolean isDns() {
    return dns;
  }

  public Optional<Integer> protostellarPort() {
    return protostellarPort;
  }

  @Override
  public String toString() {
    return "TestNodeConfig{" +
      "hostname='" + hostname + '\'' +
      ", ports=" + ports +
      '}';
  }
}

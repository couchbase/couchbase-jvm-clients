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

package com.couchbase.client.core.env;

import com.couchbase.client.core.annotation.Stability;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

@Stability.Volatile
public class SeedNode {

  public static final Set<SeedNode> DEFAULT = new HashSet<>(Collections.singletonList(
    SeedNode.create("127.0.0.1")
  ));

  private final String address;

  private final Optional<Integer> kvPort;
  private final Optional<Integer> httpPort;

  public static SeedNode create(String address) {
    return create(address, Optional.empty(), Optional.empty());
  }

  public static SeedNode create(String address, Optional<Integer> kvPort, Optional<Integer> httpPort) {
    return new SeedNode(address, kvPort, httpPort);
  }

  private SeedNode(String address, Optional<Integer> kvPort, Optional<Integer> httpPort) {
    notNullOrEmpty(address, "Address");
    notNull(kvPort, "KvPort");
    notNull(httpPort, "HttpPort");
    this.address = address;
    this.kvPort = kvPort;
    this.httpPort = httpPort;
  }

  public String address() {
    return address;
  }

  public Optional<Integer> kvPort() {
    return kvPort;
  }

  public Optional<Integer> httpPort() {
    return httpPort;
  }

  @Override
  public String toString() {
    return "SeedNode{" +
      "address='" + address + '\'' +
      ", kvPort=" + kvPort +
      ", httpPort=" + httpPort +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SeedNode seedNode = (SeedNode) o;
    return Objects.equals(address, seedNode.address) &&
      Objects.equals(kvPort, seedNode.kvPort) &&
      Objects.equals(httpPort, seedNode.httpPort);
  }

  @Override
  public int hashCode() {
    return Objects.hash(address, kvPort, httpPort);
  }
}

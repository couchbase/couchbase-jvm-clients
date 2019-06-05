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
package com.couchbase.client.core.node;

import java.util.Objects;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

/**
 * Identifies a node uniquely in the cluster.
 *
 * <p>If you ask yourself: why is the hostname not enough? Well, let me tell you that
 * it is possible to run multiple nodes on the same host if you compile it from source and know how. So the
 * hostname alone is not enough, we also need to know the cluster manager port to properly identify each
 * node.</p>
 */
public class NodeIdentifier {

  private final String address;
  private final int managerPort;

  public NodeIdentifier(final String address, final int managerPort) {
    this.address = address;
    this.managerPort = managerPort;
  }

  public String address() {
    return address;
  }

  public int managerPort() {
    return managerPort;
  }

  @Override
  public String toString() {
    return "NodeIdentifier{" +
      "address=" + redactSystem(address) +
      ", managerPort=" + redactSystem(managerPort) +
      '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NodeIdentifier that = (NodeIdentifier) o;
    return managerPort == that.managerPort && Objects.equals(address, that.address);
  }

  @Override
  public int hashCode() {
    return Objects.hash(address, managerPort);
  }

}

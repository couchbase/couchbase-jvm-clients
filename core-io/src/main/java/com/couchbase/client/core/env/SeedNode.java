package com.couchbase.client.core.env;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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

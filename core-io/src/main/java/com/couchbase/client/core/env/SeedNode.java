package com.couchbase.client.core.env;

import java.util.Optional;

public class SeedNode {

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

}

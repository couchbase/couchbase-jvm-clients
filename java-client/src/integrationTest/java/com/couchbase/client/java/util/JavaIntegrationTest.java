package com.couchbase.client.java.util;

import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.test.ClusterAwareIntegrationTest;
import com.couchbase.client.test.Services;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class JavaIntegrationTest extends ClusterAwareIntegrationTest {

  public ClusterEnvironment.Builder environment() {
    Set<SeedNode> seeds = config().nodes().stream().map(cfg -> SeedNode.create(
      cfg.hostname(),
      Optional.of(cfg.ports().get(Services.KV)),
      Optional.of(cfg.ports().get(Services.MANAGER))
    )).collect(Collectors.toSet());

    return ClusterEnvironment
      .builder(config().adminUsername(), config().adminPassword())
      .seedNodes(seeds);
  }
}

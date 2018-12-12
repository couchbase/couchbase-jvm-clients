package com.couchbase.client.core.util;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.test.ClusterAwareIntegrationTest;
import com.couchbase.client.test.Services;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class CoreIntegrationTest extends ClusterAwareIntegrationTest {

  /**
   * Creates an environment builder that already has all the needed properties to bootstrap
   * set.
   */
  public CoreEnvironment.Builder environment() {
    Set<SeedNode> seeds = config().nodes().stream().map(cfg -> SeedNode.create(
      cfg.hostname(),
      Optional.of(cfg.ports().get(Services.KV)),
      Optional.of(cfg.ports().get(Services.MANAGER))
    )).collect(Collectors.toSet());

    return CoreEnvironment
      .builder(config().adminUsername(), config().adminPassword())
      .seedNodes(seeds);
  }

}

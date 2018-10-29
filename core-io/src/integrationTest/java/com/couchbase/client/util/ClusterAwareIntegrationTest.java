package com.couchbase.client.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Parent class which drives all dynamic integration tests based on the configured
 * cluster setup.
 *
 * @since 2.0.0
 */
@ExtendWith(ClusterInvocationProvider.class)
public class ClusterAwareIntegrationTest {

  private TestClusterConfig config;

  @BeforeEach
  void setup(TestClusterConfig config) {
    this.config = config;
  }

  /**
   * Returns the current config for the integration test cluster.
   */
  public TestClusterConfig config() {
    return config;
  }

}

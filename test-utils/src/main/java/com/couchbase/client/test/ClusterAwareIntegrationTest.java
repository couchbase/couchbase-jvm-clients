package com.couchbase.client.test;

import org.junit.jupiter.api.BeforeAll;
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

  private static TestClusterConfig testClusterConfig;

  @BeforeAll
  static void setup(TestClusterConfig config) {
    testClusterConfig = config;
  }

  /**
   * Returns the current config for the integration test cluster.
   */
  public static TestClusterConfig config() {
    return testClusterConfig;
  }

}

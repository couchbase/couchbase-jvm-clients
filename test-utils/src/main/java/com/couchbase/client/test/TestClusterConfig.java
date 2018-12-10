/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import java.util.List;

/**
 * This configuration is populated from the cluster container and represents the
 * settings that can be used from the tests for bootstrapping their own code.
 *
 * @since 2.0.0
 */
public class TestClusterConfig {

  private final String bucketname;
  private final String adminUsername;
  private final String adminPassword;
  private final List<TestNodeConfig> nodes;

  public TestClusterConfig(String bucketname, String adminUsername, String adminPassword, List<TestNodeConfig> nodes) {
    this.bucketname = bucketname;
    this.adminUsername = adminUsername;
    this.adminPassword = adminPassword;
    this.nodes = nodes;
  }

  public String bucketname() {
    return bucketname;
  }

  public String adminUsername() {
    return adminUsername;
  }

  public String adminPassword() {
    return adminPassword;
  }

  public List<TestNodeConfig> nodes() {
    return nodes;
  }
}

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

package com.couchbase.client.java.env;

import com.couchbase.client.core.env.ConnectionStringPropertyLoader;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.env.UsernameAndPassword;

public class ClusterEnvironment extends CoreEnvironment {

  private ClusterEnvironment(Builder builder) {
    super(builder);
  }

  @Override
  protected String defaultAgentTitle() {
    return "java";
  }

  public static ClusterEnvironment create(final String username, final String password) {
    return builder(username, password).build();
  }

  public static ClusterEnvironment create(final Credentials credentials) {
    return builder(credentials).build();
  }

  public static ClusterEnvironment create(final String connectionString, String username, String password) {
    return builder(connectionString, username, password).build();
  }

  public static ClusterEnvironment create(final String connectionString, Credentials credentials) {
    return builder(connectionString, credentials).build();
  }

  public static ClusterEnvironment.Builder builder(final String username, final String password) {
    return builder(new UsernameAndPassword(username, password));
  }

  public static ClusterEnvironment.Builder builder(final Credentials credentials) {
    return new ClusterEnvironment.Builder(credentials);
  }

  public static ClusterEnvironment.Builder builder(final String connectionString, final String username, final String password) {
    return builder(connectionString, new UsernameAndPassword(username, password));
  }

  public static ClusterEnvironment.Builder builder(final String connectionString, final Credentials credentials) {
    return builder(credentials).load(new ConnectionStringPropertyLoader(connectionString));
  }

  public static class Builder extends CoreEnvironment.Builder<Builder> {

    Builder(Credentials credentials) {
      super(credentials);
    }

    public Builder load(final ClusterPropertyLoader loader) {
      loader.load(this);
      return this;
    }

    public ClusterEnvironment build() {
      return new ClusterEnvironment(this);
    }
  }
}

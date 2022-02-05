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

package com.couchbase.client.java;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.java.env.ClusterEnvironment;

import java.util.function.Consumer;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Allows specifying custom options when connecting to the cluster.
 */
public class ClusterOptions {

  private ClusterEnvironment environment;
  private Consumer<ClusterEnvironment.Builder> environmentCustomizer;
  private final Authenticator authenticator;

  private ClusterOptions(final Authenticator authenticator) {
    this.authenticator = authenticator;
  }

  public static ClusterOptions clusterOptions(final Authenticator authenticator) {
    notNull(authenticator, "Authenticator");
    return new ClusterOptions(authenticator);
  }

  public static ClusterOptions clusterOptions(final String username, final String password) {
    notNullOrEmpty(username, "Username");
    notNullOrEmpty(password, "Password");
    return clusterOptions(PasswordAuthenticator.create(username, password));
  }

  private static final String environmentAlreadyConfigured =
      "environment(ClusterEnvironment) and environment(Consumer<ClusterEnvironment.Builder>)" +
          " are mutually exclusive; call one or the other, but not both.";

  /**
   * Sets the ClusterEnvironment to use with this cluster.
   * <p>
   * The caller is responsible for shutting down the environment after
   * all clusters sharing it have disconnected.
   * <p>
   * Use this method when sharing an environment between multiple clusters.
   * In all other cases, prefer {@link #environment(Consumer)}.
   */
  public ClusterOptions environment(final ClusterEnvironment environment) {
    notNull(environment, "ClusterEnvironment");
    if (this.environmentCustomizer != null) {
      throw new IllegalStateException(environmentAlreadyConfigured);
    }
    this.environment = environment;
    return this;
  }

  /**
   * Sets a callback that configures the ClusterEnvironment owned by this
   * cluster.
   * <p>
   * The cluster will manage the lifecycle of the environment, and
   * automatically shut it down when the cluster is disconnected.
   * <p>
   * This is the recommended way to configure the cluster environment
   * unless you need to share an environment between multiple clusters.
   */
  @Stability.Uncommitted
  public ClusterOptions environment(final Consumer<ClusterEnvironment.Builder> environmentCustomizer) {
    notNull(environmentCustomizer, "environmentCustomizer");
    if (this.environment != null) {
      throw new IllegalStateException(environmentAlreadyConfigured);
    }
    if (this.environmentCustomizer != null) {
      throw new IllegalStateException("environment(Consumer<ClusterEnvironment.Builder>) may only be called once.");
    }
    this.environmentCustomizer = environmentCustomizer;
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built {

    Built() { }

    public Authenticator authenticator() {
      return authenticator;
    }

    public ClusterEnvironment environment() {
      return environment;
    }

    public Consumer<ClusterEnvironment.Builder> environmentCustomizer() {
      return environmentCustomizer;
    }

  }

}

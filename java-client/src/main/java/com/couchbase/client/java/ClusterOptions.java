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

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Allows to specify custom options when connecting to the cluster.
 */
public class ClusterOptions {

  private ClusterEnvironment environment;
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

  public ClusterOptions environment(final ClusterEnvironment environment) {
    notNull(environment, "ClusterEnvironment");
    this.environment = environment;
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built {

    public Authenticator authenticator() {
      return authenticator;
    }

    public ClusterEnvironment environment() {
      return environment;
    }

  }

}

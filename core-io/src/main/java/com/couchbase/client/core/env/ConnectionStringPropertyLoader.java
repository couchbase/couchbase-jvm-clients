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

package com.couchbase.client.core.env;

import com.couchbase.client.core.util.ConnectionString;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * This {@link PropertyLoader} takes a connection string and applies all properties
 * that are supported and it knows about.
 *
 * @since 2.0.0
 */
public class ConnectionStringPropertyLoader implements PropertyLoader<CoreEnvironment.Builder> {

  /**
   * Holds the connection string provided by the application.
   */
  private final ConnectionString connectionString;

  public ConnectionStringPropertyLoader(final String connectionString) {
    this.connectionString = ConnectionString.create(connectionString);
  }

  @Override
  public void load(final CoreEnvironment.Builder builder) {
    Set<String> seeds = connectionString
      .allHosts()
      .stream()
      .map(a -> a.getAddress().getHostAddress())
      .collect(Collectors.toSet());

    builder.seedNodes(seeds);
  }
}

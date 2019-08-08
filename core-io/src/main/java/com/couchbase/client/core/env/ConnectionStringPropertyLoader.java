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
import com.couchbase.client.core.util.DnsSrv;

import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  private final BuilderPropertySetter setter = new BuilderPropertySetter();

  public ConnectionStringPropertyLoader(final String connectionString) {
    this.connectionString = ConnectionString.create(connectionString);
  }

  @Override
  public void load(final CoreEnvironment.Builder builder) {
    if (builder.ioConfig().dnsSrvEnabled() && connectionString.isValidDnsSrv()) {
      boolean isEncrypted = connectionString.scheme() == ConnectionString.Scheme.COUCHBASES;
      String dnsHostname = connectionString.hosts().get(0).hostname();
      try {
        List<String> foundNodes = DnsSrv.fromDnsSrv("", false, isEncrypted, dnsHostname);
        if (foundNodes.isEmpty()) {
          throw new IllegalStateException("The loaded DNS SRV list from " + dnsHostname + " is empty!");
        }
        builder.seedNodes(foundNodes.stream().map(SeedNode::create).collect(Collectors.toSet()));
      } catch (Exception ex) {
        builder.seedNodes(populateSeedsFromConnectionString());
      }
    } else {
      builder.seedNodes(populateSeedsFromConnectionString());
    }

    for (Map.Entry<String, String> entry : connectionString.params().entrySet()) {
      try {
        setter.set(builder, entry.getKey(), entry.getValue());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
          "Failed to apply connection string property \"" + entry.getKey() + "\". " + e.getMessage(), e);
      }
    }
  }

  private Set<SeedNode> populateSeedsFromConnectionString() {
    return connectionString
      .hosts()
      .stream()
      .map(a -> SeedNode.create(
        a.hostname(),
        a.port() > 0 ? Optional.of(a.port()) : Optional.empty(),
        Optional.empty()
      ))
      .collect(Collectors.toSet());
  }

}

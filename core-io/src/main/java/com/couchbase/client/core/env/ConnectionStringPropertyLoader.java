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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * This {@link PropertyLoader} takes a connection string and applies all properties
 * that are supported and it knows about.
 *
 * @since 2.0.0
 */
public class ConnectionStringPropertyLoader extends AbstractMapPropertyLoader<CoreEnvironment.Builder> {

  /**
   * Holds the connection string provided by the application.
   */
  private final ConnectionString connectionString;

  /**
   * Holds alias values from other connection string implementations (like libcouchbase or the SDK 3 Foundation RFC)
   */
  private static final Map<String, String> COMPAT_ALIAS = new HashMap<>();

  static {
    COMPAT_ALIAS.put("certpath", "security.trustCertificate");
    COMPAT_ALIAS.put("enable_tls", "security.enableTls");

    COMPAT_ALIAS.put("kv_connect_timeout", "timeout.connectTimeout");
    COMPAT_ALIAS.put("kv_timeout", "timeout.kvTimeout");
    COMPAT_ALIAS.put("kv_durable_timeout", "timeout.kvDurableTimeout");
    COMPAT_ALIAS.put("view_timeout", "timeout.viewTimeout");
    COMPAT_ALIAS.put("query_timeout", "timeout.queryTimeout");
    COMPAT_ALIAS.put("analytics_timeout", "timeout.analyticsTimeout");
    COMPAT_ALIAS.put("search_timeout", "timeout.searchTimeout");
    COMPAT_ALIAS.put("management_timeout", "timeout.managementTimeout");

    COMPAT_ALIAS.put("enable_mutation_tokens", "io.enableMutationTokens");
    COMPAT_ALIAS.put("tcp_keepalive_time", "io.tcpKeepAliveTime");
    COMPAT_ALIAS.put("enable_tcp_keepalives", "io.enableTcpKeepAlives");
    COMPAT_ALIAS.put("config_poll_interval", "io.configPollInterval");
    COMPAT_ALIAS.put("config_idle_redial_timeout", "io.configIdleRedialTimeout");
    COMPAT_ALIAS.put("num_kv_connections", "io.numKvConnections");
    COMPAT_ALIAS.put("max_http_connections", "io.maxHttpConnections");
    COMPAT_ALIAS.put("idle_http_connection_timeout", "io.idleHttpConnectionTimeout");
  }

  public ConnectionStringPropertyLoader(final String connectionString) {
    this(ConnectionString.create(connectionString));
  }

  public ConnectionStringPropertyLoader(final ConnectionString connectionString) {
    this.connectionString = requireNonNull(connectionString);
  }

  @Override
  protected Map<String, String> propertyMap() {
    final Map<String, String> properties = connectionString
      .params()
      .entrySet()
      .stream()
      .collect(Collectors.toMap(
        entry -> COMPAT_ALIAS.getOrDefault(entry.getKey(), entry.getKey()),
        Map.Entry::getValue
      ));

    if (connectionString.scheme() == ConnectionString.Scheme.COUCHBASES) {
      properties.put("security.enableTls", "true");
    }

    return properties;
  }

}

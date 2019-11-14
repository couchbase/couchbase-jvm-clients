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

import java.util.HashMap;
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

  /**
   * Holds the dynamic setter to build the properties.
   */
  private final BuilderPropertySetter setter = new BuilderPropertySetter();

  /**
   * Holds alias values from other connection string implementations (like libcouchbase)
   */
  private static final Map<String, String> COMPAT_ALIAS = new HashMap<>();

  static {
    COMPAT_ALIAS.put("certpath", "security.trustCertificate");
  }

  public ConnectionStringPropertyLoader(final String connectionString) {
    this.connectionString = ConnectionString.create(connectionString);
  }

  @Override
  public void load(final CoreEnvironment.Builder builder) {
    if (connectionString.scheme() == ConnectionString.Scheme.COUCHBASES) {
      setter.set(builder, "security.enableTls", "true");
    }

    for (Map.Entry<String, String> entry : connectionString.params().entrySet()) {
      try {
        String key = entry.getKey();
        if (COMPAT_ALIAS.containsKey(key)) {
          key = COMPAT_ALIAS.get(key);
        }
        setter.set(builder, key, entry.getValue());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
          "Failed to apply connection string property \"" + entry.getKey() + "\". " + e.getMessage(), e);
      }
    }
  }



}

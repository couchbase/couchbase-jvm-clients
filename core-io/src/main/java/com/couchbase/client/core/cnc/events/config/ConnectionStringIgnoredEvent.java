/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.events.config;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.util.ConnectionString;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * @deprecated Instead of publishing this event, the SDK now throws an exception.
 * Because this class never graduated from "volatile API" status, it will be
 * removed from the next minor version of the SDK.
 */
@Stability.Volatile
@Deprecated
public class ConnectionStringIgnoredEvent extends AbstractEvent {
  private final ConnectionString connectionString;
  private final String description;

  private ConnectionStringIgnoredEvent(final ConnectionString connectionString, String description) {
    super(Severity.WARN, Category.CONFIG, Duration.ZERO, null);

    this.connectionString = requireNonNull(connectionString);
    this.description = requireNonNull(description);
  }

  @Deprecated
  public static ConnectionStringIgnoredEvent ignoringScheme(ConnectionString connectionString) {
    return new ConnectionStringIgnoredEvent(connectionString,
        "The connection string specifies the secure 'couchbases' scheme," +
            " but TLS will not be used because the cluster was created from" +
            " a shared ClusterEnvironment that does not have 'security.enableTls' set to true."
    );
  }

  @Deprecated
  public static ConnectionStringIgnoredEvent ignoringParameters(ConnectionString connectionString) {
    return new ConnectionStringIgnoredEvent(connectionString,
        "The connection string has parameters, but they will be ignored because " +
            "the cluster was created from a shared ClusterEnvironment.");
  }

  public ConnectionString connectionString() {
    return connectionString;
  }

  @Override
  public String description() {
    return description;
  }
}
